#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <cmath>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <ctime>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <double-conversion/double-conversion.h> // already a dep of arrow; and printf won't do
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include "common.h"


// Batch size determines RAM usage and I/O.
//
// Lower value might mean "more seeking" (though we have not tested that).
// Higher value means "more RAM".
//
// parquet-to-web is designed for streaming data over the Internet. So
// [adamhooper, 2019-09-23] my intuition is that the client side is slow;
// therefore, I lean towards low RAM, high seeking.
static const int BATCH_SIZE = 100;


// [adamhooper, 2019-09-25] a shoddy use of inheritance here
// (Printer/CsvPrinter/JsonPrinter). Just making sure code is shared. This
// could certainly be cleaned.
struct Printer : public arrow::ArrayVisitor {
  FILE* fp;

  Printer (FILE* aFp)
    : fp(aFp)
    , columnName(nullptr)
    , columnIndex(0)
    , arrayIndex(0)
    , doubleBuilder(this->doubleBuffer, this->kBufferSize)
    , doubleConverter(double_conversion::DoubleToStringConverter::EcmaScriptConverter())
  {
    std::fill(this->doubleBuffer, this->doubleBuffer + this->kBufferSize, 0);
  }

  void prepare(int aColumnIndex, const std::string* aColumnName, int aArrayIndex) {
    this->columnIndex = aColumnIndex;
    this->columnName = aColumnName;
    this->arrayIndex = aArrayIndex;
  }

  virtual void writeFileHeader() = 0; // JSON '['
  virtual void writeFileFooter() = 0; // JSON ']'
  virtual void writeRecordStart(bool isFirst) = 0; // JSON '{'; CSV '\n'
  virtual void writeRecordStop() = 0; // JSON '}'
  virtual void writeFieldStart() = 0; // JSON field name; CSV comma
  virtual void writeHeaderField() = 0; // CSV field name
  virtual void writeNull() = 0; // CSV '', JSON 'null'
  virtual void writeString(const std::string& value) = 0; // escaped
  virtual void writeTimestamp(int64_t value, arrow::TimeUnit::type timeUnit) = 0;

  void writeRawShortISO8601UTCTimestamp(int64_t value, arrow::TimeUnit::type timeUnit) {
    int64_t epochSeconds;
    int subsecondFraction;
    int nFractionDigits;
    switch (timeUnit) {
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
        std::cerr << "Failure: unsupported time unit " << timeUnit << std::endl;
        std::_Exit(1);
    }

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
  }

#define VISIT_PRINTF(arrayType, format) \
  arrow::Status Visit(const arrayType& array) { \
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
        fwrite_unlocked(this->doubleBuffer, 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // Text mode: NaN, +inf and -inf are all null (empty string)
      this->writeNull();
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
        fwrite_unlocked(this->doubleBuffer, 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // Text mode: NaN, +inf and -inf are all null (empty string)
      this->writeNull();
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
    this->writeString(value);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampArray& array) override {
    const int64_t value = array.Value(this->arrayIndex);
    const arrow::TimestampType* type = dynamic_cast<arrow::TimestampType*>(array.type().get());
    this->writeTimestamp(value, type->unit());
    return arrow::Status::OK();
  }

protected:
  const std::string* columnName;
  int columnIndex;
  int arrayIndex;

private:
  static const int kBufferSize = 128; // the number in https://github.com/google/double-conversion/blob/master/test/cctest/test-conversions.cc
  char doubleBuffer[kBufferSize];
  double_conversion::StringBuilder doubleBuilder;
  const double_conversion::DoubleToStringConverter& doubleConverter;
};


struct CsvPrinter : public Printer {
  CsvPrinter(FILE* aFp) : Printer(aFp) {}

  void writeFileHeader() override {}
  void writeFileFooter() override {}
  void writeRecordStop() override {}

  void writeRecordStart(bool isFirst) override {
    fputc_unlocked('\n', this->fp); // newline -- start new CSV record
  }

  void writeFieldStart() override {
    if (this->columnIndex > 0) {
      fputc_unlocked(',', this->fp);
    }
  }

  void writeHeaderField() override {
    this->writeFieldStart();
    this->writeString(*this->columnName);
  }

  void writeNull() override {
    // CSV: null is empty string. Write nothing.
  }

  void writeString(const std::string& value) override {
    bool needQuote = false;
    for (const char& c: value) {
        // assume UTF-8 -- it's okay to ascii-compare it
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needQuote = true;
            break;
        }
    }

    if (!needQuote) {
      fwrite_unlocked(value.c_str(), 1, value.size(), this->fp);
    } else {
      fputc_unlocked('"', this->fp);
      size_t nWritten = 0;
      while (nWritten < value.size()) {
        const size_t quote_pos = value.find('"', nWritten);
        if (quote_pos == std::string::npos) {
          // No more quotation marks
          fwrite_unlocked(value.c_str() + nWritten, 1, value.size() - nWritten, this->fp);
          nWritten = value.size();
        } else {
          fwrite_unlocked(value.c_str() + nWritten, 1, quote_pos - nWritten, this->fp);
          fwrite_unlocked("\"\"", 1, 2, this->fp);
          nWritten = quote_pos + 1;
        }
      }
      fputc_unlocked('"', this->fp);
    }
  }

  void writeTimestamp(int64_t value, arrow::TimeUnit::type timeUnit) override {
    this->writeRawShortISO8601UTCTimestamp(value, timeUnit);
  }
};


struct JsonPrinter : public Printer {
  JsonPrinter(FILE* aFp) : Printer(aFp) {}

  void writeFileHeader() override {
    fputc_unlocked('[', this->fp); // begin array
  }

  void writeFileFooter() override {
    fputc_unlocked(']', this->fp); // end array
  }

  void writeRecordStart(bool isFirst) override {
    if (!isFirst) {
      fputc_unlocked(',', this->fp);
    }
    fputc_unlocked('{', this->fp); // begin object
  }

  void writeRecordStop() override {
    fputc_unlocked('}', this->fp); // end object
  }

  void writeFieldStart() override {
    if (this->columnIndex > 0) {
      fputc_unlocked(',', this->fp);
    }
    this->writeString(*this->columnName);
    fputc_unlocked(':', this->fp);
  }

  void writeHeaderField() override {
    // JSON has no header
  }

  virtual void writeNull() override {
    fwrite_unlocked("null", 1, 4, this->fp);
  }

  void writeString(const std::string& value) override {
    fputc_unlocked('"', this->fp);
    for (const char& c: value) {
      // assume UTF-8 -- it's okay to ascii-compare it
      switch (c) {
        case '"': fwrite_unlocked("\\\"", 1, 2, this->fp); break;
        case '\\': fwrite_unlocked("\\\\", 1, 2, this->fp); break;
        case '\b': fwrite_unlocked("\\b", 1, 2, this->fp); break;
        case '\f': fwrite_unlocked("\\f", 1, 2, this->fp); break;
        case '\n': fwrite_unlocked("\\n", 1, 2, this->fp); break;
        case '\r': fwrite_unlocked("\\r", 1, 2, this->fp); break;
        case '\t': fwrite_unlocked("\\t", 1, 2, this->fp); break;
        default:
          if ('\0' <= c && c <= '\x1f') {
            fprintf(this->fp, "\\u%04hhd", c);
          } else {
            fputc_unlocked(c, this->fp);
          }
      }
    }
    fputc_unlocked('"', this->fp);
  }

  void writeTimestamp(int64_t value, arrow::TimeUnit::type timeUnit) override {
    fputc_unlocked('"', this->fp);
    this->writeRawShortISO8601UTCTimestamp(value, timeUnit);
    fputc_unlocked('"', this->fp);
  }
};


struct ColumnIterator {
  int columnIndex;
  const std::string name;
  int rowIndex;
  int nRows;
  std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
  int batchIndex;
  std::shared_ptr<arrow::Array> batch;

  ColumnIterator(int aColumnIndex, const std::string& aName, int aNRows, std::unique_ptr<parquet::arrow::ColumnReader> aReader)
    : columnIndex(aColumnIndex), name(aName), rowIndex(-1), nRows(aNRows), columnReader(std::move(aReader)), batchIndex(-1), batch(nullptr)
  {
  }

  bool advanceToNextValueIfAvailable() {
    this->batchIndex++;
    this->rowIndex++;
    if (this->rowIndex == this->nRows) {
      if (this->batch) {
        this->batch.reset();
      }
      return false;
    }
    if (this->batch.get() == nullptr || this->batchIndex >= this->batch->length()) {
      std::shared_ptr<arrow::ChunkedArray> columnChunks;
      ASSERT_ARROW_OK(this->columnReader->NextBatch(std::min(BATCH_SIZE, this->nRows - this->rowIndex), &columnChunks), "reading batch");
      if (columnChunks.get() == nullptr || columnChunks->length() == 0) {
        this->batch.reset();
        return false;
      } else {
        ASSERT_ARROW_OK(chunkedArrayToArray(*columnChunks, &this->batch), "concatenating column chunks");
        // If it's a dict, decode it. (It would be nice to decode one value at a time, but that's
        // hard to achieve with the Arrow API. And it's not decoding in batches is _bad_....)
        ASSERT_ARROW_OK(decodeIfDictionary(&this->batch), "decoding dictionary values");
        this->batchIndex = 0;
      }
    }

    return true;
  }

  void printCurrentValue(Printer& printer) {
    printer.prepare(this->columnIndex, &this->name, this->batchIndex);
    printer.writeFieldStart();
    if (this->batch->IsNull(this->batchIndex)) {
      printer.writeNull();
    } else {
      ASSERT_ARROW_OK(this->batch->Accept(&printer), "getting value");
    }
  }
};


static void streamParquet(const std::string& path, const std::string& format, FILE* fp) {
  std::shared_ptr<arrow::io::MemoryMappedFile> parquetFile;
  ASSERT_ARROW_OK(arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ, &parquetFile), "opening Parquet file");
  std::unique_ptr<parquet::arrow::FileReader> arrowReader;
  ASSERT_ARROW_OK(parquet::arrow::OpenFile(parquetFile, arrow::default_memory_pool(), &arrowReader), "creating Parquet reader");
  arrowReader->set_use_threads(false);

  std::shared_ptr<arrow::Schema> schema;
  ASSERT_ARROW_OK(arrowReader->GetSchema(&schema), "parsing Parquet schema");

  int nRows = arrowReader->parquet_reader()->metadata()->num_rows();

  std::vector<std::unique_ptr<ColumnIterator>> columnIterators(schema->num_fields());
  for (int i = 0; i < columnIterators.size(); i++) {
    std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
    ASSERT_ARROW_OK(arrowReader->GetColumn(i, &columnReader), "getting column");
    columnIterators[i].reset(new ColumnIterator(i, schema->field(i)->name(), nRows, std::move(columnReader)));
  }

  std::unique_ptr<Printer> printer;
  if (format == "json") {
    printer.reset(new JsonPrinter(fp));
  } else {
    printer.reset(new CsvPrinter(fp));
  }

  // Write headers
  printer->writeFileHeader();
  for (const auto& columnIterator : columnIterators) {
    printer->prepare(columnIterator->columnIndex, &columnIterator->name, columnIterator->batchIndex);
    printer->writeHeaderField();
  }
  if (columnIterators.size() > 0) {
    bool isFirst = true;
    while (true) {
      for (auto& columnIterator : columnIterators) {
        if (!columnIterator->advanceToNextValueIfAvailable()) {
          goto footer;
        }
      }
      printer->writeRecordStart(isFirst);
      for (auto& columnIterator : columnIterators) {
        columnIterator->printCurrentValue(*printer);
      }
      printer->writeRecordStop();
      isFirst = false;
    }
  }
footer:
  printer->writeFileFooter();
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

  streamParquet(parquetPath, format, stdout);

  return 0;
}
