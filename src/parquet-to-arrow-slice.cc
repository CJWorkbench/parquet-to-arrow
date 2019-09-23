#include <algorithm>
#include <exception>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>


static const int SKIP_MAX_BATCH_SIZE = 10000; // when seeking, how quickly do we seek? Higher costs more RAM.


static inline void ASSERT_ARROW_OK(arrow::Status status, const char* message) {
  if (!status.ok()) {
    std::cerr << "Failure " << message << ": " << status.ToString() << std::endl;
    std::terminate();
  }
}


/**
 * A pair of characters on the unsigned-integer number line.
 *
 * * Start comes before stop.
 * * Start is inclusive, and the first valid value is 0.
 * * Stop is exclusive.
 * * If start == stop, the range is empty.
 */
struct Range {
  int start;
  int stop;

  Range(int aStart, int aStop) : start(aStart), stop(aStop) {}

  /**
   * Parse a Range from a C string.
   *
   * Throw std::invalid_argument or std::out_of_range if input is not valid.
   */
  static Range parse(const std::string& s) {
    std::size_t pos(0);
    const int start = static_cast<int>(std::stoul(s, &pos)); // throw invalid_argument/out_of_range
    if (pos >= s.length() || s[pos] != '-') {
      throw std::invalid_argument("Range requires '-' character (as in, '0-12')");
    }
    const int stop = static_cast<int>(std::stoul(s.substr(pos + 1))); // throw invalid_argument/out_of_range
    if (start > stop) {
      throw std::invalid_argument("Range's start must come before its stop (as in, '0-12')");
    }
    return Range(start, stop);
  }

  int size() const {
    return this->stop - this->start;
  }

  std::vector<int> indices() const {
    std::vector<int> ret(this->stop - this->start);
    std::iota(ret.begin(), ret.end(), this->start);
    return ret;
  }

  Range clip(int max) const {
    return Range(std::min(this->start, max), std::min(this->stop, max));
  }
};

static std::shared_ptr<arrow::Table> readParquet(const std::string& path, const Range& columnRange, const Range& rowRange) {
  std::shared_ptr<arrow::io::MemoryMappedFile> parquetFile;
  ASSERT_ARROW_OK(arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ, &parquetFile), "opening Parquet file");
  std::unique_ptr<parquet::arrow::FileReader> arrowReader;
  ASSERT_ARROW_OK(parquet::arrow::OpenFile(parquetFile, arrow::default_memory_pool(), &arrowReader), "creating Parquet reader");
  arrowReader->set_use_threads(false);

  // Clip request range to file contents
  const std::shared_ptr<parquet::FileMetaData> parquetMetadata = arrowReader->parquet_reader()->metadata();
  const Range clippedColumnRange = columnRange.clip(parquetMetadata->num_columns());
  const Range clippedRowRange = rowRange.clip(parquetMetadata->num_rows());

  std::shared_ptr<arrow::Schema> arrowSchema;
  ASSERT_ARROW_OK(arrowReader->GetSchema(clippedColumnRange.indices(), &arrowSchema), "converting to Arrow schema");

  std::vector<std::shared_ptr<arrow::Column>> columns;
  for (int i = clippedColumnRange.start; i < clippedColumnRange.stop; i++) {
    std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
    ASSERT_ARROW_OK(arrowReader->GetColumn(i, &columnReader), "opening column");
    std::shared_ptr<arrow::ChunkedArray> columnChunks;
    int toSkip = clippedRowRange.start;
    while (toSkip > 0) {
      const int batchSize = std::min(toSkip, SKIP_MAX_BATCH_SIZE);
      ASSERT_ARROW_OK(columnReader->NextBatch(batchSize, &columnChunks), "skipping column data");
      columnChunks = nullptr; // free RAM
      toSkip -= batchSize;
    }
    std::shared_ptr<arrow::Array> columnArray;
    ASSERT_ARROW_OK(columnReader->NextBatch(clippedRowRange.size(), &columnChunks), "reading column chunks");
    ASSERT_ARROW_OK(arrow::Concatenate(columnChunks->chunks(), arrow::default_memory_pool(), &columnArray), "concatenating column chunks");
    const std::shared_ptr<arrow::Field> field = arrowSchema->field(i - clippedColumnRange.start);
    // Do not copy any metadata from the Parquet file. Use
    // field->name(), not field.
    std::shared_ptr<arrow::Column> column(new arrow::Column(field->name(), columnArray));
    columns.push_back(column);
  }

  std::shared_ptr<arrow::Table> arrowTable = arrow::Table::Make(columns, clippedRowRange.size());

  return arrowTable;
}


static void writeArrow(const arrow::Table& arrowTable, const std::string& path) {
  std::shared_ptr<arrow::io::FileOutputStream> outputStream;
  ASSERT_ARROW_OK(arrow::io::FileOutputStream::Open(path, &outputStream), "opening output stream");
  std::shared_ptr<arrow::ipc::RecordBatchWriter> fileWriter;
  ASSERT_ARROW_OK(arrow::ipc::RecordBatchFileWriter::Open(outputStream.get(), arrowTable.schema(), &fileWriter), "creating file writer");
  ASSERT_ARROW_OK(fileWriter->WriteTable(arrowTable), "writing Arrow table");
  ASSERT_ARROW_OK(fileWriter->Close(), "closing Arrow file");
}


int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_FILENAME COL0-COLN ROW0-ROWN ARROW_FILENAME" << std::endl;
    std::cerr << std::endl;
    std::cerr << "For instance: " << argv[0] << " table.parquet 0-16 200-400" << std::endl;
    std::cerr << "Rows and columns are numbered like C arrays. Out-of-bounds indices are ignored." << std::endl;
    return 1;
  }

  const std::string parquetPath(argv[1]);
  const Range columnRange(Range::parse(std::string(argv[2])));
  const Range rowRange(Range::parse(std::string(argv[3])));
  const std::string arrowPath(argv[4]);

  std::shared_ptr<arrow::Table> arrowTable(readParquet(parquetPath, columnRange, rowRange));
  writeArrow(*arrowTable, arrowPath);

  return 0;
}
