#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

#include "common.h"


static std::shared_ptr<arrow::Table> readParquet(const std::string& path) {
  std::unique_ptr<parquet::ParquetFileReader> parquetFileReader;
  parquet::ArrowReaderProperties arrowReaderProperties(false); // do not use threads

  try {
    parquetFileReader = parquet::ParquetFileReader::OpenFile(path); // raises?

    // Decide which columns to read as dictionary
    // (Arrow seems good at interpreting its _own_ written parquet dictionaries;
    // but not fastparquet-written dictionaries. Maybe because encoding=4 is
    // missing? [adamhooper, 2019-10-09] I can't figure it out.)
    if (parquetFileReader->metadata()->num_row_groups() > 0) {
      std::shared_ptr<parquet::RowGroupReader> rowGroupReader(parquetFileReader->RowGroup(0));
      const parquet::RowGroupMetaData* rowGroupMetaData(rowGroupReader->metadata());

      for (int i = 0; i < rowGroupMetaData->num_columns(); i++) {
        const std::unique_ptr<parquet::ColumnChunkMetaData> columnMetaData(rowGroupMetaData->ColumnChunk(i));
        arrowReaderProperties.set_read_dictionary(i, columnMetaData->has_dictionary_page());
      }
    }
  } catch (const parquet::ParquetException& ex) {
    std::cerr << ex.what() << std::endl;
    std::_Exit(1);
  }

  std::unique_ptr<parquet::arrow::FileReader> parquetArrowReader;
  ASSERT_ARROW_OK(parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(parquetFileReader), arrowReaderProperties, &parquetArrowReader), "creating Parquet reader");

  std::shared_ptr<arrow::Schema> schema;
  ASSERT_ARROW_OK(parquetArrowReader->GetSchema(&schema), "converting to Arrow schema");
  // Clear metadata. (If we're reading from fastparquet there's nonsense
  // metadata in the Parquet file.)
  schema = schema->RemoveMetadata();

  // Output a single-record-batch array: read a column at a time
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for (int i = 0; i < schema->num_fields(); i++) {
    std::shared_ptr<arrow::ChunkedArray> chunkedArray;
    ASSERT_ARROW_OK(parquetArrowReader->ReadColumn(i, &chunkedArray), "reading column");
    std::shared_ptr<arrow::Array> columnArray = chunkedArrayToArray(*chunkedArray);
    arrays.push_back(columnArray);
  }

  return arrow::Table::Make(schema, arrays);
}


int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_FILENAME ARROW_FILENAME" << std::endl;
    return 1;
  }

  const std::string parquetPath(argv[1]);
  const std::string arrowPath(argv[2]);

  std::shared_ptr<arrow::Table> arrowTable(readParquet(parquetPath));
  writeArrowTable(*arrowTable, arrowPath);

  return 0;
}
