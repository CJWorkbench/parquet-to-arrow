#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <vector>
#include <cmath>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>  // assert(), setenv()
#include <ctime>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/thread_pool.h>
#include <double-conversion/double-conversion.h> // already a dep of arrow; and printf won't do
#include <gflags/gflags.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include "common.h"
#include "range.h"

static bool
validate_range(const char* flagname, const std::string& value)
{
  if (value == "") return true;

  auto [_, ec] = parse_range(&*value.cbegin(), &*value.cend());
  if (ec != std::errc()) {
    std::cerr << flagname << " does not look like '123-234': " << std::make_error_code(ec) << std::endl;
    return false;
  }

  return true;
}

DEFINE_string(row_range, "", "[start, end) range of rows to include");
DEFINE_validator(row_range, &validate_range);
DEFINE_string(column_range, "", "[start, end) range of columns to include");
DEFINE_validator(column_range, &validate_range);


/* Batch size determines RAM usage and I/O.
 *
 * Lower value means more I/O operations. Higher value means larger RAM
 * footprint.
 *
 * Benchmarking on 63MB, 70-col, 1M-row text file, with some dictionary-encoded
 * columns on Intel(R) Core(TM) i5-6600K CPU @ 3.50GHz and command:
 *
 *   docker run -it --rm -v $(pwd):/data \
 *     $(docker build . --target cpp-build -q) \
 *     sh -c 'time ./parquet-to-text-stream /data/test.parquet csv > /dev/null'
 *
 * * BATCH_SIZE=10 => ~4.0s
 * * BATCH_SIZE=20 => ~3.8s
 * * BATCH_SIZE=30 => ~3.6s
 * * BATCH_SIZE=50 => ~3.5s
 * * BATCH_SIZE=100 => ~3.5s
 * * BATCH_SIZE=500 => ~3.4s
 * * BATCH_SIZE=1000 => ~3.4s
 * * BATCH_SIZE=2000 => ~3.75s
 * * BATCH_SIZE=5000 => ~3.9s
 *
 * parquet-to-text-stream is designed for streaming data over the Internet. We
 * value time-to-first-byte (low BATCH_SIZE) and low RAM usage (low BATCH_SIZE).
 * Per-column batch size can be rather large (64kb per text column), so err on
 * the low side (while still trying to impress your friends, naturally).
 */
static const int BATCH_SIZE = 30;


struct TimestampMillis { int64_t value; };
struct TimestampMicros { int64_t value; };
struct TimestampNanos { int64_t value; };


template<typename PhysicalType, typename PrintableType>
PrintableType physical_to_printable(PhysicalType value)
{
  return static_cast<PrintableType>(value);
}

template<>
TimestampMillis physical_to_printable(int64_t value)
{
  return TimestampMillis { value };
}

template<>
TimestampMicros physical_to_printable(int64_t value)
{
  return TimestampMicros { value };
}

template<>
TimestampNanos physical_to_printable(int64_t value)
{
  return TimestampNanos { value };
}

template<>
std::string_view physical_to_printable(parquet::ByteArray value) {
  return std::string_view(reinterpret_cast<const char*>(value.ptr), value.len);
}


template<typename ColumnReaderType_, typename PrintableType_>
class BufferedColumnReader {
public:
  typedef ColumnReaderType_ ColumnReaderType;
  typedef typename ColumnReaderType::T PhysicalType;
  typedef PrintableType_ PrintableType;

private:
  std::shared_ptr<ColumnReaderType> parquetReader;
  std::array<PhysicalType, BATCH_SIZE> batch;
  // `batchValid`: Arrow bitset must "be able to store 1 bit more than required"
  // 0 values => 1 byte
  // 1 value => 1 byte
  // 7 values => 1 byte
  // 8 values => 2 bytes
  // 9 values => 2 bytes
  std::array<uint8_t, BATCH_SIZE / 8 + 1> batchValid;
  int64_t batchSize;
  int64_t batchCursor;

public:

  BufferedColumnReader(std::shared_ptr<ColumnReaderType> parquetReader_)
    : parquetReader(parquetReader_)
    , batchSize(0)
    , batchCursor(0)
  {
    assert(parquetReader->descr()->max_definition_level() == 1);
    assert(parquetReader->descr()->max_repetition_level() == 0);
  }

  void skipRows(int64_t toSkip) {
    int64_t skipInBatch = std::min(toSkip, this->batchSize - this->batchCursor);

    // Skip within the batch
    this->batchCursor += skipInBatch;
    toSkip -= skipInBatch;

    // Skip _past_ the batch
    [[maybe_unused]] auto nSkipped = this->parquetReader->Skip(toSkip);
    assert(nSkipped == toSkip);
  }

  /**
   * Return the next value, or std::nullopt if it is null.
   *
   * Undefined behavior if there is no next element.
   */
  std::optional<PrintableType> next() {
    if (this->batchCursor >= this->batchSize) {
      this->rebuffer();

      // Crash if calling next() when hasNext() is false
      assert(this->batchCursor < this->batchSize);
    }

    std::optional<PrintableType> ret;
    bool isValid = arrow::BitUtil::GetBit(&this->batchValid[0], this->batchCursor);
    if (isValid) { // "valid" means "not-null"
      ret = physical_to_printable<PhysicalType, PrintableType>(this->batch[this->batchCursor]);
    }
    this->batchCursor++;
    return ret;
  }

private:
  void rebuffer() {
    std::array<int16_t, BATCH_SIZE> def_levels;
    int64_t levels_read;
    int64_t values_read;
    int64_t null_count;

    this->batchSize = this->parquetReader->ReadBatchSpaced(
      BATCH_SIZE,
      &def_levels[0], // non-required fields have max_definition_level==1
      nullptr,
      &this->batch[0],
      &this->batchValid[0],
      0, // valid_bits_offset
      &levels_read,
      &values_read,
      &null_count
    );
    this->batchCursor = 0;
  }
};

using BufferedFloatColumnReader = BufferedColumnReader<parquet::FloatReader, float>;
using BufferedDoubleColumnReader = BufferedColumnReader<parquet::DoubleReader, double>;
using BufferedInt32ColumnReader = BufferedColumnReader<parquet::Int32Reader, int32_t>;
using BufferedInt64ColumnReader = BufferedColumnReader<parquet::Int64Reader, int64_t>;
using BufferedUint32ColumnReader = BufferedColumnReader<parquet::Int32Reader, uint32_t>;
using BufferedUint64ColumnReader = BufferedColumnReader<parquet::Int64Reader, uint64_t>;
using BufferedStringColumnReader = BufferedColumnReader<parquet::ByteArrayReader, std::string_view>;
using BufferedTimestampMillisColumnReader = BufferedColumnReader<parquet::Int64Reader, TimestampMillis>;
using BufferedTimestampMicrosColumnReader = BufferedColumnReader<parquet::Int64Reader, TimestampMicros>;
using BufferedTimestampNanosColumnReader = BufferedColumnReader<parquet::Int64Reader, TimestampNanos>;


template<typename BufferedReaderType>
class FileColumnIterator
{
public:
  typedef typename BufferedReaderType::ColumnReaderType ColumnReaderType;
  typedef typename BufferedReaderType::PrintableType PrintableType;

private:
  parquet::ParquetFileReader& fileReader;
  std::unique_ptr<BufferedReaderType> currentReader;
  int columnIndex;
  std::string_view name; // lasts as long as the fileReader
  int currentRowGroup;
  int currentReaderCursor;
  int currentReaderSize;

public:
  FileColumnIterator(parquet::ParquetFileReader& fileReader, int columnIndex_)
    : fileReader(fileReader)
    , columnIndex(columnIndex_)
    , name(fileReader.metadata()->schema()->Column(columnIndex_)->name())
    , currentRowGroup(-1) // incremented to 0 in ctor, in loadNextRowGroup()
    , currentReaderCursor(0)
    , currentReaderSize(0)

  {
    this->loadNextRowGroup();
  }

  std::string_view getName() const {
    return this->name;
  }

  void skipRows(int64_t toSkip) {
    while (toSkip > this->currentReaderSize - this->currentReaderCursor)
    {
      toSkip -= (this->currentReaderSize - this->currentReaderCursor);
      this->loadNextRowGroup();
    }
    this->currentReader->skipRows(toSkip);
    this->currentReaderCursor += toSkip;
  }

  /**
   * Return the next value, or std::nullopt if it is null.
   *
   * Undefined behavior if there is no next element.
   */
  std::optional<PrintableType> next() {
    if (this->currentReaderCursor >= this->currentReaderSize)
    {
      this->loadNextRowGroup();
      assert(this->currentReaderCursor < this->currentReaderSize);
    }

    this->currentReaderCursor++;
    return this->currentReader->next();
  }

private:
  void loadNextRowGroup() {
    this->currentRowGroup++;
    std::shared_ptr<parquet::RowGroupReader> rowGroupReader(this->fileReader.RowGroup(this->currentRowGroup));
    std::shared_ptr<parquet::ColumnReader> columnReader(rowGroupReader->Column(this->columnIndex));
    std::shared_ptr<ColumnReaderType> typedColumnReader = std::dynamic_pointer_cast<ColumnReaderType>(columnReader);
    if (!typedColumnReader) {
      throw std::runtime_error(
        std::string("Could not cast column reader ") + columnReader->descr()->ToString() + " to desired type"
      );
    }
    this->currentReader = std::make_unique<BufferedReaderType>(typedColumnReader);
    this->currentReaderCursor = 0;
    this->currentReaderSize = rowGroupReader->metadata()->num_rows();
  }
};


class Printer {
  static const int kBufferSize = 128; // the number in https://github.com/google/double-conversion/blob/master/test/cctest/test-conversions.cc
  std::array<char, kBufferSize> doubleBuffer;
  double_conversion::StringBuilder doubleBuilder;
  const double_conversion::DoubleToStringConverter& doubleConverter;
protected:
  FILE* fp;

public:
  Printer(FILE* fp_)
    : doubleBuilder(&this->doubleBuffer[0], this->kBufferSize)
    , doubleConverter(double_conversion::DoubleToStringConverter::EcmaScriptConverter())
    , fp(fp_)
  {
  }

  virtual void writeFileHeader() = 0; // JSON '['
  virtual void writeFileFooter() = 0; // JSON ']'
  virtual void writeRecordStart(int rowIndex) = 0; // JSON '{'; CSV '\n'
  virtual void writeRecordStop() = 0; // JSON '}'
  virtual void writeFieldStart(int columnIndex, std::string_view name) = 0; // JSON field name; CSV comma
  virtual void writeHeaderField(int columnIndex, std::string_view name) = 0; // CSV field name

  virtual void writeNull() = 0; // CSV '', JSON 'null'
  virtual void writeString(std::string_view value) = 0; // escaped

  void write(TimestampMillis value) { this->writeTimestamp(value.value, 3); }
  void write(TimestampMicros value) { this->writeTimestamp(value.value, 6); }
  void write(TimestampNanos value) { this->writeTimestamp(value.value, 9); }
  void write(std::string_view value) { this->writeString(value); }

  // It just so happens JSON and CSV write numbers exactly the same way:
  void write(float value) {
    if (std::isfinite(value)) {
      this->doubleBuilder.Reset();
      if (this->doubleConverter.ToShortestSingle(value, &this->doubleBuilder)) {
        // No need to call this->doubleBuilder.Finalize() because we know
        // where the string ends.
        fwrite_unlocked(&this->doubleBuffer[0], 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // Text mode: NaN, +inf and -inf are all null (empty string)
      this->writeNull();
    }
  }

  void write(double value) {
    if (std::isfinite(value)) {
      this->doubleBuilder.Reset();
      if (this->doubleConverter.ToShortest(value, &this->doubleBuilder)) {
        // No need to call this->doubleBuilder.Finalize() because we know
        // where the string ends.
        fwrite_unlocked(&this->doubleBuffer[0], 1, this->doubleBuilder.position(), this->fp);
      } else {
        std::cerr << "Failed to convert float: " << value << std::endl;
        // I guess we can recover from this. According to the docs, there's no
        // way for this to ever happen anyway.
      }
    } else {
      // Text mode: NaN, +inf and -inf are all null (empty string)
      this->writeNull();
    }
  }

  void write(int32_t value) { fprintf(this->fp, "%" PRIi32, value); }
  void write(int64_t value) { fprintf(this->fp, "%" PRIi64, value); }
  void write(uint32_t value) { fprintf(this->fp, "%" PRIu32, value); }
  void write(uint64_t value) { fprintf(this->fp, "%" PRIu64, value); }

protected:

  virtual void writeTimestamp(int64_t value, int nFractionDigits) = 0;

  void writeRawShortISO8601UTCTimestamp(int64_t value, int nFractionDigits) {
    int64_t epochSeconds;
    int subsecondFraction;
    switch (nFractionDigits) {
      case 3:
        epochSeconds = value / 1000;
        subsecondFraction = value % 1000;
        if (value < 0  && subsecondFraction != 0) {
          epochSeconds -= 1;
          subsecondFraction = (subsecondFraction + 1000) % 1000;
        }
        break;
      case 6:
        epochSeconds = value / 1000000;
        subsecondFraction = value % 1000000;
        if (value < 0  && subsecondFraction != 0) {
          epochSeconds -= 1;
          subsecondFraction = (subsecondFraction + 1000000) % 1000000;
        }
        break;
      case 9:
        epochSeconds = value / 1000000000;
        subsecondFraction = value % 1000000000;
        if (value < 0  && subsecondFraction != 0) {
          epochSeconds -= 1;
          subsecondFraction = (subsecondFraction + 1000000000) % 1000000000;
        }
        break;
      default:
        std::cerr << "Failure: unsupported nFractionDigits " << nFractionDigits << std::endl;
        std::_Exit(1);
    }

    struct tm time = { .tm_sec=0, .tm_min=0, .tm_hour=0, .tm_mday=0, .tm_mon=0, .tm_year=0, .tm_wday=0, .tm_yday=0, .tm_isdst=0 };
    const time_t timeInput = static_cast<time_t>(epochSeconds);
    gmtime_r(&timeInput, &time);

    // We always print date
    fprintf(this->fp, "%04d-%02d-%02d", time.tm_year + 1900, time.tm_mon + 1, time.tm_mday);

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
};


struct CsvPrinter : public Printer {
  CsvPrinter(FILE* aFp) : Printer(aFp) {}

  void writeFileHeader() override {}
  void writeFileFooter() override {}
  void writeRecordStop() override {}

  void writeRecordStart(int rowIndex) override {
    fputc_unlocked('\n', this->fp); // newline -- start new CSV record
  }

  void writeFieldStart(int columnIndex, std::string_view name) override {
    if (columnIndex > 0) {
      fputc_unlocked(',', this->fp);
    }
  }

  void writeHeaderField(int columnIndex, std::string_view name) override {
    this->writeFieldStart(columnIndex, name);
    this->writeString(name);
  }

  void writeNull() override {
    // CSV: null is empty string. Write nothing.
  }

  void writeString(std::string_view value) override {
    bool needQuote = false;
    for (const char& c: value) {
        // assume UTF-8 -- it's okay to ascii-compare it
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needQuote = true;
            break;
        }
    }

    if (!needQuote) {
      fwrite_unlocked(value.data(), 1, value.size(), this->fp);
    } else {
      fputc_unlocked('"', this->fp);
      size_t nWritten = 0;
      while (nWritten < value.size()) {
        const size_t quote_pos = value.find('"', nWritten);
        if (quote_pos == std::string::npos) {
          // No more quotation marks
          fwrite_unlocked(value.data() + nWritten, 1, value.size() - nWritten, this->fp);
          nWritten = value.size();
        } else {
          fwrite_unlocked(value.data() + nWritten, 1, quote_pos - nWritten, this->fp);
          fwrite_unlocked("\"\"", 1, 2, this->fp);
          nWritten = quote_pos + 1;
        }
      }
      fputc_unlocked('"', this->fp);
    }
  }

  void writeTimestamp(int64_t value, int nFractionDigits) override {
    this->writeRawShortISO8601UTCTimestamp(value, nFractionDigits);
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

  void writeRecordStart(int rowIndex) override {
    if (rowIndex != 0) {
      fputc_unlocked(',', this->fp);
    }
    fputc_unlocked('{', this->fp); // begin object
  }

  void writeRecordStop() override {
    fputc_unlocked('}', this->fp); // end object
  }

  void writeFieldStart(int columnIndex, std::string_view name) override {
    if (columnIndex > 0) {
      fputc_unlocked(',', this->fp);
    }
    this->writeString(name);
    fputc_unlocked(':', this->fp);
  }

  void writeHeaderField(int columnIndex, std::string_view name) override {
    // JSON has no header
  }

  void writeNull() override {
    fwrite_unlocked("null", 1, 4, this->fp);
  }

  void writeString(std::string_view value) override {
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

  void writeTimestamp(int64_t value, int nFractionDigits) override {
    fputc_unlocked('"', this->fp);
    this->writeRawShortISO8601UTCTimestamp(value, nFractionDigits);
    fputc_unlocked('"', this->fp);
  }
};


class Transcriber
{
public:
  Transcriber(Printer& printer_) : printer(printer_) {}

  // https://en.cppreference.com/w/cpp/memory/unique_ptr:
  // If T is a derived class of some base B, then std::unique_ptr<T> is
  // implicitly convertible to std::unique_ptr<B>. The default deleter of the
  // resulting std::unique_ptr<B> will use operator delete for B, leading to
  // undefined behavior unless the destructor of B is virtual.
  virtual ~Transcriber() {}

  /**
   * Skip nRows values.
   *
   * Undefined behavior if there are not that many values to skip.
   */
  virtual void skipRows(int64_t nRows) = 0;

  /**
   * Print the next value.
   *
   * Undefined behavior if there is no next element.
   */
  virtual void printNext(size_t outputColumnIndex) = 0;

  /**
   * Print the header field (CSV-only).
   */
  virtual void printHeaderField(size_t outputColumnIndex) = 0;

protected:
  Printer& printer;
};


template<typename FileColumnIteratorType>
class BufferedTranscriber : public Transcriber
{
public:
  typedef typename FileColumnIteratorType::PrintableType PrintableType;

private:
  std::unique_ptr<FileColumnIteratorType> reader;

public:
  BufferedTranscriber(Printer& printer, std::unique_ptr<FileColumnIteratorType> reader_)
    : Transcriber(printer)
    , reader(std::move(reader_))
  {
  }

  void printNext(size_t outputColumnIndex) override
  {
    this->printer.writeFieldStart(outputColumnIndex, this->reader->getName());

    std::optional<PrintableType> valueOrNull = this->reader->next();
    if (valueOrNull.has_value()) {
      this->printer.write(valueOrNull.value());
    } else {
      this->printer.writeNull();
    }
  }

  void skipRows(int64_t nRows) override
  {
    this->reader->skipRows(nRows);
  }

  void printHeaderField(size_t outputColumnIndex) override
  {
    this->printer.writeHeaderField(outputColumnIndex, this->reader->getName());
  }
};

template<typename BufferedReaderType>
static std::unique_ptr<Transcriber>
makeTranscriber(parquet::ParquetFileReader& fileReader, int columnIndex, Printer& printer)
{
  typedef FileColumnIterator<BufferedReaderType> FileColumnIteratorType;
  typedef BufferedTranscriber<FileColumnIteratorType> TranscriberType;

  auto fileColumnIterator = std::make_unique<FileColumnIteratorType>(fileReader, columnIndex);
  auto transcriber = std::make_unique<TranscriberType>(printer, std::move(fileColumnIterator));
  return std::move(transcriber);
}


static std::unique_ptr<Transcriber>
makeTranscriberForIntColumn(parquet::ParquetFileReader& fileReader, int columnIndex, Printer& printer)
{
  const auto descr = fileReader.metadata()->schema()->Column(columnIndex);
  const parquet::LogicalType* logicalType = descr->logical_type().get();

  if (logicalType->type() == parquet::LogicalType::Type::TIMESTAMP) {
    const auto timestampType = dynamic_cast<const parquet::TimestampLogicalType*>(logicalType);
    if (!timestampType) {
      throw std::runtime_error("TIMESTAMP column did not convert to TimestampLogicalType");
    }
    // We ignore timestampType->is_adjusted_to_utc(): an obvious codepath like
    // pa.array([], type=pa.timestamp(unit="ns")) isn't adjusted to UTC, so
    // there's plenty of UTC data in the wild that isn't read as such.
    //
    // <opinionated>It would be an error in judgment for a developer to create
    // a non-UTC timestamp, since one such value does not always represent one
    // point in time. We won't pay any more attention to such
    // shenanigans.</opinionated>

    switch (timestampType->time_unit()) {
      case parquet::LogicalType::TimeUnit::MILLIS:
        return makeTranscriber<BufferedTimestampMillisColumnReader>(fileReader, columnIndex, printer);
      case parquet::LogicalType::TimeUnit::MICROS:
        return makeTranscriber<BufferedTimestampMicrosColumnReader>(fileReader, columnIndex, printer);
      case parquet::LogicalType::TimeUnit::NANOS:
        return makeTranscriber<BufferedTimestampNanosColumnReader>(fileReader, columnIndex, printer);
      default:
        throw std::runtime_error("Unknown TimeUnit in a TIMESTAMP column");
    }
  } else if (
    logicalType->type() == parquet::LogicalType::Type::INT
    // "NONE" means, signed-int
    || logicalType->type() == parquet::LogicalType::Type::NONE
  ) {
    const auto intType = dynamic_cast<const parquet::IntLogicalType*>(logicalType);
    // If logicalType->type() == NONE, then there's no intType; we assume signed
    bool isSigned = (intType == nullptr || intType->is_signed());

    // We don't care about intType->bit_width(): we handle numbers based on
    // their _physical_ type, and Parquet only stores int32 and int64

    switch (descr->physical_type()) {
      case parquet::Type::INT32:
        return isSigned
          ? makeTranscriber<BufferedInt32ColumnReader>(fileReader, columnIndex, printer)
          : makeTranscriber<BufferedUint32ColumnReader>(fileReader, columnIndex, printer);
      case parquet::Type::INT64:
        return isSigned
          ? makeTranscriber<BufferedInt64ColumnReader>(fileReader, columnIndex, printer)
          : makeTranscriber<BufferedUint64ColumnReader>(fileReader, columnIndex, printer);
      default:
        throw new std::logic_error("unreachable: physical type is not INT32 or INT64");
    }
  } else {
    throw new std::runtime_error(
      std::string("For INT32 and INT64, we only handle INT and TIMESTAMP types; got ")
      + logicalType->ToString()
    );
  }
}

static std::unique_ptr<Transcriber>
makeTranscriberForByteArrayColumn(parquet::ParquetFileReader& fileReader, int columnIndex, Printer& printer)
{
  const auto descr = fileReader.metadata()->schema()->Column(columnIndex);
  const auto logicalType = descr->logical_type();
  switch (logicalType->type()) {
    case parquet::LogicalType::Type::STRING:
      return makeTranscriber<BufferedStringColumnReader>(fileReader, columnIndex, printer);
    default:
      throw std::runtime_error(
        std::string("For BYTE_ARRAY, we only handle STRING type; got ") + logicalType->ToString()
      );
  }
}

static std::unique_ptr<Transcriber>
makeTranscriberForColumn(parquet::ParquetFileReader& fileReader, int columnIndex, Printer& printer)
{
  const auto descr = fileReader.metadata()->schema()->Column(columnIndex);
  assert(descr->max_definition_level() == 1);
  assert(descr->max_repetition_level() == 0);
  switch (descr->physical_type()) {
    case parquet::Type::INT32:
    case parquet::Type::INT64:
      return makeTranscriberForIntColumn(fileReader, columnIndex, printer);
    case parquet::Type::FLOAT:
      return makeTranscriber<BufferedFloatColumnReader>(fileReader, columnIndex, printer);
    case parquet::Type::DOUBLE:
      return makeTranscriber<BufferedDoubleColumnReader>(fileReader, columnIndex, printer);
    case parquet::Type::BYTE_ARRAY:
      return makeTranscriberForByteArrayColumn(fileReader, columnIndex, printer);
    default:
      throw std::runtime_error(std::string("Cannot read physical type: ") + descr->ToString());
  }
}


static void
streamParquet(const std::string& path, Printer& printer, Range columnRange, Range rowRange) {
  std::unique_ptr<parquet::ParquetFileReader> fileReader(
    parquet::ParquetFileReader::OpenFile(path)
  );

  columnRange = columnRange.clip(fileReader->metadata()->num_columns());
  rowRange = rowRange.clip(fileReader->metadata()->num_rows());

  std::vector<std::unique_ptr<Transcriber>> transcribers(columnRange.size());
  for (size_t i = 0; i < transcribers.size(); i++) {
    size_t columnIndex = columnRange.start + i;
    std::unique_ptr<Transcriber> transcriber(makeTranscriberForColumn(*fileReader, columnIndex, printer));
    transcriber->skipRows(static_cast<int64_t>(rowRange.start));
    transcribers[i] = std::move(transcriber);
  }

  // Write headers
  printer.writeFileHeader();
  if (transcribers.size() > 0) {
    // Write headers
    for (size_t outputColumnIndex = 0; outputColumnIndex < columnRange.size(); outputColumnIndex++) {
      transcribers[outputColumnIndex]->printHeaderField(outputColumnIndex);
    }

    // Write rows
    for (auto rowIndex = rowRange.start; rowIndex < rowRange.stop; rowIndex++) {
      printer.writeRecordStart(rowIndex - rowRange.start);

      for (size_t outputColumnIndex = 0; outputColumnIndex < columnRange.size(); outputColumnIndex++) {
        transcribers[outputColumnIndex]->printNext(outputColumnIndex);
      }
      printer.writeRecordStop();
    }
  }
  printer.writeFileFooter();
}


int main(int argc, char** argv) {
  std::string usage = std::string("Usage: ") + argv[0] + " <PARQUET_FILENAME> <FORMAT>";
  gflags::SetUsageMessage(usage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    gflags::ShowUsageWithFlags(argv[0]);
    return 1;
  }

  const std::string parquetPath(argv[1]);
  const std::string formatString(argv[2]);

  Range columnRange;
  if (FLAGS_column_range != "") {
    columnRange = parse_range(&*FLAGS_column_range.cbegin(), &*FLAGS_column_range.cend()).range;
  }
  Range rowRange;
  if (FLAGS_row_range != "") {
    rowRange = parse_range(&*FLAGS_row_range.cbegin(), &*FLAGS_row_range.cend()).range;
  }

  if (formatString == "csv") {
    CsvPrinter printer(stdout);
    streamParquet(parquetPath, printer, columnRange, rowRange);
  } else if (formatString == "json") {
    JsonPrinter printer(stdout);
    streamParquet(parquetPath, printer, columnRange, rowRange);
  } else {
    std::cerr << "<FORMAT> must be either 'csv' or 'json'" << std::endl;
    gflags::ShowUsageWithFlags(argv[0]);
    return 1;
  }

  return 0;
}
