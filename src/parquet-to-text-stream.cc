//#include <algorithm>
//#include <exception>
#include <iostream>
#include <memory>
//#include <numeric>
#include <string>
#include <vector>
#include <cmath>
#include <cinttypes>
#include <cstdio>
#include <ctime>

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <double-conversion/double-conversion.h> // already a dep of arrow; and printf won't do
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>


// Batch size determines RAM usage and I/O.
//
// Lower value might mean "more seeking" (though we have not tested that).
// Higher value means "more RAM".
//
// parquet-to-web is designed for streaming data over the Internet. So
// [adamhooper, 2019-09-23] my intuition is that the client side is slow;
// therefore, I lean towards low RAM, high seeking.
static const int BATCH_SIZE = 100;

static inline void ASSERT_ARROW_OK(arrow::Status status, const char* message) {
  if (!status.ok()) {
    std::cerr << "Failure " << message << ": " << status.ToString() << std::endl;
    std::terminate();
  }
}


struct Printer : public arrow::ArrayVisitor {
  FILE* fp;
  const std::string* columnName;
  int arrayIndex;

  Printer(FILE* aFp) : fp(aFp), columnName(nullptr), arrayIndex(0) {}

  void prepare(const std::string* aColumnName, int aArrayIndex) {
    this->columnName = aColumnName;
    this->arrayIndex = aArrayIndex;
  }

  virtual void VisitNullValue() = 0;
};


struct CsvPrinter : public Printer {
  CsvPrinter(FILE* aFp)
      : Printer(aFp)
      , doubleBuilder(this->doubleBuffer, this->kBufferSize)
      , doubleConverter(double_conversion::DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN,
                        "inf", "nan", 'e', -8, 21, 0, 0)
  {
    std::fill(this->doubleBuffer, this->doubleBuffer + this->kBufferSize, 0);
  }

  void VisitNullValue() {
    // There is no null in CSVs. Output the empty string -- which is a no-op
  }

  void AdvanceColumn() {
    fputc(',', this->fp);
  }

  void AdvanceRow() {
    fputc('\n', this->fp);
  }

  void WriteString(const std::string& value) {
    bool needQuote = false;
    for (const char& c: value) {
        // assume UTF-8 -- it's okay to ascii-compare it
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needQuote = true;
            break;
        }
    }

    if (!needQuote) {
      fwrite(value.c_str(), 1, value.size(), this->fp);
    } else {
      fputc('"', this->fp);
      size_t nWritten = 0;
      while (nWritten < value.size()) {
        const size_t quote_pos = value.find('"', nWritten);
        if (quote_pos == std::string::npos) {
          // No more quotation marks
          fwrite(value.c_str() + nWritten, 1, value.size() - nWritten, this->fp);
          nWritten = value.size();
        } else {
          fwrite(value.c_str() + nWritten, 1, quote_pos - nWritten, this->fp);
          fwrite("\"\"", 1, 2, this->fp);
          nWritten = quote_pos + 1;
        }
      }
      fputc('"', this->fp);
    }
  }

#define VISIT_PRINTF(arrayType, format) \
  arrow::Status Visit(const arrayType& array) override { \
    const arrayType::value_type value = array.Value(this->arrayIndex); \
    fprintf(this->fp, format, value); \
    return arrow::Status::OK(); \
  }

  VISIT_PRINTF(arrow::Int8Array, "%" PRIi8)
  VISIT_PRINTF(arrow::Int16Array, "%" PRIi16)
  VISIT_PRINTF(arrow::Int32Array, "%" PRIi32)
  VISIT_PRINTF(arrow::Int64Array, "%" PRIi64)
  VISIT_PRINTF(arrow::UInt8Array, "%" PRIu8)
  VISIT_PRINTF(arrow::UInt16Array, "%" PRIu16)
  VISIT_PRINTF(arrow::UInt32Array, "%" PRIu32)
  VISIT_PRINTF(arrow::UInt64Array, "%" PRIu64)
#undef VISIT_PRINTF

  arrow::Status Visit(const arrow::FloatArray& array) override {
    float value = array.Value(this->arrayIndex);
    if (std::isfinite(value)) {
      this->doubleBuilder.Reset();
      if (this->doubleConverter.ToShortestSingle(value, &this->doubleBuilder)) {
        // No need to call this->doubleBuilder.Finalize() because we know
        // where the string ends.
        fwrite(this->doubleBuffer, 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // CSV format: NaN, +inf and -inf are all null (empty string)
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleArray& array) override {
    double value = array.Value(this->arrayIndex);
    if (std::isfinite(value)) {
      this->doubleBuilder.Reset();
      if (this->doubleConverter.ToShortest(value, &this->doubleBuilder)) {
        // No need to call this->doubleBuilder.Finalize() because we know
        // where the string ends.
        fwrite(this->doubleBuffer, 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // CSV format: NaN, +inf and -inf are all null (empty string)
    }
    return arrow::Status::OK();
  }

#define VISIT_TODO(arrayType) \
  arrow::Status Visit(const arrayType& array) override { \
    return arrow::Status::NotImplemented("TODO: unhandled column type"); \
  }

  VISIT_TODO(arrow::ExtensionArray)
  VISIT_TODO(arrow::DictionaryArray) // we decode dictionaries while reading
  VISIT_TODO(arrow::UnionArray)
  VISIT_TODO(arrow::StructArray)
  VISIT_TODO(arrow::FixedSizeListArray)
  VISIT_TODO(arrow::MapArray)
  VISIT_TODO(arrow::ListArray)
  VISIT_TODO(arrow::DurationArray)
#undef VISIT_TODO

  arrow::Status Visit(const arrow::StringArray& array) override {
    const std::string value = array.GetString(this->arrayIndex);
    this->WriteString(value);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampArray& array) override {
    const int64_t value = array.Value(this->arrayIndex);
    const arrow::TimestampType* type = dynamic_cast<arrow::TimestampType*>(array.type().get());
    int64_t epochSeconds;
    int subsecondFraction;
    int nFractionDigits;
    switch (type->unit()) {
      case arrow::TimeUnit::SECOND:
        epochSeconds = value;
        subsecondFraction = 0;
        nFractionDigits = 0;
        break;
      case arrow::TimeUnit::MILLI:
        epochSeconds = value / 1000;
        subsecondFraction = value % 1000;
        nFractionDigits = 3;
        break;
      case arrow::TimeUnit::MICRO:
        epochSeconds = value / 1000000;
        subsecondFraction = value % 1000000;
        nFractionDigits = 6;
        break;
      case arrow::TimeUnit::NANO:
        epochSeconds = value / 1000000000;
        subsecondFraction = value % 1000000000;
        nFractionDigits = 9;
        break;
      default:
        std::cerr << "Failure: unsupported time unit " << type->unit() << std::endl;
        std::terminate();
    }

    // TODO use the correct time zone!
    struct tm time = { .tm_sec=0, .tm_min=0, .tm_hour=0, .tm_mday=0, .tm_mon=0, .tm_year=0, .tm_wday=0, .tm_yday=0, .tm_isdst=0 };
    const time_t timeInput = static_cast<time_t>(epochSeconds);
    gmtime_r(&timeInput, &time);

    // We always print date
    fprintf(this->fp, "%-04d-%02d-%02d", time.tm_year + 1900, time.tm_mon + 1, time.tm_mday);

    // "Auto-format" time: only print the resolution it uses.
    //
    // This is perfect for CSV, because it uses fewer characters, transmits
    // the same information, adheres to ISO8601, and is easier to read.
    //
    // * If ns=0, only show us (YYYY-MM-DDTHH:MM:SS.ssssss)
    // * If us=0, only show ms (YYYY-MM-DDTHH:MM:SS.sss)
    // * If ms=0, only show s (YYYY-MM-DDTHH:MM:SS)
    // * If h=0, m=0, s=0, only show date (YYYY-MM-DD)
    while (nFractionDigits > 0 && subsecondFraction % 1000 == 0) {
      subsecondFraction /= 1000;
      nFractionDigits -= 3;
    }
    if (nFractionDigits == 0) {
      if (time.tm_hour != 0 || time.tm_min != 0 || time.tm_sec != 0) {
        fprintf(this->fp, "T%02d:%02d:%02dZ", time.tm_hour, time.tm_min, time.tm_sec);
      } else {
        // It's 00:00:00.000000000Z -- don't print any of it
      }
    } else if (nFractionDigits == 3) {
      fprintf(this->fp, "T%02d:%02d:%02d.%03dZ", time.tm_hour, time.tm_min, time.tm_sec, subsecondFraction);
    } else if (nFractionDigits == 6) {
      fprintf(this->fp, "T%02d:%02d:%02d.%06dZ", time.tm_hour, time.tm_min, time.tm_sec, subsecondFraction);
    } else if (nFractionDigits == 9) {
      fprintf(this->fp, "T%02d:%02d:%02d.%09dZ", time.tm_hour, time.tm_min, time.tm_sec, subsecondFraction);
    }
    return arrow::Status::OK();
  }

private:
  static const int kBufferSize = 128; // the number in https://github.com/google/double-conversion/blob/master/test/cctest/test-conversions.cc
  char doubleBuffer[kBufferSize];
  double_conversion::StringBuilder doubleBuilder;
  double_conversion::DoubleToStringConverter doubleConverter;
};


struct ColumnIterator {
  const std::string name;
  std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
  int batchIndex;
  std::shared_ptr<arrow::Array> batch;

  ColumnIterator(const std::string& aName, std::unique_ptr<parquet::arrow::ColumnReader> aReader)
    : name(aName), columnReader(std::move(aReader)), batchIndex(0), batch(nullptr)
  {
  }

  bool advanceToNextValueIfAvailable() {
    this->batchIndex++;
    if (this->batch.get() == nullptr || this->batchIndex >= this->batch->length()) {
      std::shared_ptr<arrow::ChunkedArray> columnChunks;
      ASSERT_ARROW_OK(this->columnReader->NextBatch(BATCH_SIZE, &columnChunks), "reading batch");
      if (columnChunks.get() == nullptr || columnChunks->length() == 0) {
        this->batch.reset();
        return false;
      } else {
        if (columnChunks->num_chunks() == 1) {
            this->batch = columnChunks->chunk(0);
        } else {
            ASSERT_ARROW_OK(Concatenate(columnChunks->chunks(), arrow::default_memory_pool(), &this->batch), "concatenating column chunks");
        }
        this->batchIndex = 0;
      }
    }

    return true;
  }

  void printCurrentValue(Printer& printer) {
    printer.prepare(&this->name, this->batchIndex);
    if (this->batch->IsNull(this->batchIndex)) {
      printer.VisitNullValue();
    } else {
      ASSERT_ARROW_OK(this->batch->Accept(&printer), "getting value");
    }
  }
};


static void streamParquetCsv(const std::string& path, FILE* fp) {
  std::shared_ptr<arrow::io::MemoryMappedFile> parquetFile;
  ASSERT_ARROW_OK(arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ, &parquetFile), "opening Parquet file");
  std::unique_ptr<parquet::arrow::FileReader> arrowReader;
  ASSERT_ARROW_OK(parquet::arrow::OpenFile(parquetFile, arrow::default_memory_pool(), &arrowReader), "creating Parquet reader");
  arrowReader->set_use_threads(false);

  std::shared_ptr<arrow::Schema> schema;
  ASSERT_ARROW_OK(arrowReader->GetSchema(&schema), "parsing Parquet schema");

  std::vector<std::unique_ptr<ColumnIterator>> columnIterators(schema->num_fields());
  for (int i = 0; i < columnIterators.size(); i++) {
    std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
    ASSERT_ARROW_OK(arrowReader->GetColumn(i, &columnReader), "getting column");
    columnIterators[i].reset(new ColumnIterator(schema->field(i)->name(), std::move(columnReader)));
  }

  CsvPrinter printer(fp);

  // Write headers
  for (int i = 0; i < columnIterators.size(); i++) {
    if (i > 0) {
      printer.AdvanceColumn();
    }
    printer.WriteString(columnIterators[i]->name);
  }
  if (columnIterators.size() > 0) {
    while (true) {
      for (auto it = columnIterators.begin(); it != columnIterators.end(); ++it) {
        ColumnIterator* col = it->get();
        if (!(col->advanceToNextValueIfAvailable())) {
            return; // break out of our loop
        }
        if (it == columnIterators.begin()) {
          printer.AdvanceRow();
        } else {
          printer.AdvanceColumn();
        }
        col->printCurrentValue(printer);
      }

    }
  }
}


int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_FILENAME FORMAT" << std::endl;
    std::cerr << std::endl;
    std::cerr << "For instance: " << argv[0] << " table.parquet csv" << std::endl;
    return 1;
  }

  const std::string parquetPath(argv[1]);
  const std::string format(argv[2]);

  streamParquetCsv(parquetPath, stdout);

  return 0;
}
