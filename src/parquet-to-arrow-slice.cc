#include <algorithm>
#include <exception>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include "common.h"
#include "range.h"


static const int SKIP_MAX_BATCH_SIZE = 10000; // when seeking, how quickly do we seek? Higher costs more RAM.


static std::shared_ptr<arrow::Table> readParquet(const std::string& path, const Range& columnRange, const Range& rowRange) {
  std::shared_ptr<arrow::io::MemoryMappedFile> parquetFile;
  ASSERT_ARROW_OK(arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ, &parquetFile), "opening Parquet file");
  std::unique_ptr<parquet::arrow::FileReader> parquetReader;
  ASSERT_ARROW_OK(parquet::arrow::OpenFile(parquetFile, arrow::default_memory_pool(), &parquetReader), "creating Parquet reader");
  parquetReader->set_use_threads(false);

  // Clip request range to file contents
  const std::shared_ptr<parquet::FileMetaData> parquetMetadata = parquetReader->parquet_reader()->metadata();
  const Range clippedColumnRange = columnRange.clip(parquetMetadata->num_columns());
  const Range clippedRowRange = rowRange.clip(parquetMetadata->num_rows());

  std::shared_ptr<arrow::Schema> arrowSchema;
  ASSERT_ARROW_OK(parquetReader->GetSchema(&arrowSchema), "converting to Arrow schema");

  std::vector<std::shared_ptr<arrow::Array>> columnArrays;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (int i = clippedColumnRange.start; i < clippedColumnRange.stop; i++) {
    std::unique_ptr<parquet::arrow::ColumnReader> columnReader;
    ASSERT_ARROW_OK(parquetReader->GetColumn(i, &columnReader), "opening column");
    std::shared_ptr<arrow::ChunkedArray> columnChunks;
    int toSkip = clippedRowRange.start;
    while (toSkip > 0) {
      const int batchSize = std::min(toSkip, SKIP_MAX_BATCH_SIZE);
      ASSERT_ARROW_OK(columnReader->NextBatch(batchSize, &columnChunks), "skipping column data");
      columnChunks = nullptr; // free RAM
      toSkip -= batchSize;
    }
    ASSERT_ARROW_OK(columnReader->NextBatch(clippedRowRange.size(), &columnChunks), "reading column chunks");
    std::shared_ptr<arrow::Array> columnArray;
    ASSERT_ARROW_OK(chunkedArrayToArray(*columnChunks, &columnArray), "concatenating column chunks");
    // If it's a dict, decode it. (This program outputs a _slice_, which we define as
    // small. Dictionaries can be large.)
    ASSERT_ARROW_OK(decodeIfDictionary(&columnArray), "decoding dictionary values");

    columnArrays.push_back(columnArray);

    // Do not copy any metadata from the Parquet file. Use
    // field->name(), not field.
    const auto parquetColumn(parquetMetadata->schema()->Column(i));
    const std::string& columnName = parquetColumn->name();
    std::shared_ptr<arrow::Field> field(new arrow::Field(columnName, columnArray->type(), columnArray->null_count() != 0));
    fields.push_back(field);
  }

  std::shared_ptr<arrow::Schema> outSchema(new arrow::Schema(fields));
  std::shared_ptr<arrow::Table> outTable(arrow::Table::Make(outSchema, columnArrays, clippedRowRange.size()));

  return outTable;
}


int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_FILENAME COL0-COLN ROW0-ROWN ARROW_FILENAME" << std::endl;
    std::cerr << std::endl;
    std::cerr << "For instance: " << argv[0] << " table.parquet 0-16 200-400 table.arrow" << std::endl;
    std::cerr << "Rows and columns are numbered like C arrays. Out-of-bounds indices are ignored." << std::endl;
    return 1;
  }

  const std::string parquetPath(argv[1]);
  const std::string columnRangeString(argv[2]);
  const std::string rowRangeString(argv[3]);
  auto [ columnRange, columnRangeErr ] = parse_range(&*columnRangeString.cbegin(), &*columnRangeString.cend());
  if (columnRangeErr != std::errc()) {
    std::cerr << "column range must look like '123-234': " << std::make_error_code(columnRangeErr) << std::endl;
    return 1;
  }
  auto [ rowRange, rowRangeErr ] = parse_range(&*rowRangeString.cbegin(), &*rowRangeString.cend());
  if (rowRangeErr != std::errc()) {
    std::cerr << "row range must look like '123-234': " << std::make_error_code(rowRangeErr) << std::endl;
    return 1;
  }
  const std::string arrowPath(argv[4]);

  std::shared_ptr<arrow::Table> arrowTable(readParquet(parquetPath, columnRange, rowRange));
  writeArrowTable(*arrowTable, arrowPath);

  return 0;
}
