#include <exception>
#include <iostream>
#include <memory>
#include <string>

#include <double-conversion/double-conversion.h> // already a dep of arrow; and printf won't do
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
#include <parquet/exception.h>

#include "common.h"


std::unique_ptr<parquet::ParquetFileReader> openParquetFile(const std::string& path) {
  try {
    return parquet::ParquetFileReader::OpenFile(path); // raises?
  } catch (const parquet::ParquetException& ex) {
    std::cerr << ex.what() << std::endl;
    std::_Exit(1);
  }
}


int diffColumn(int columnNumber, const parquet::ColumnDescriptor& column1, const parquet::ColumnDescriptor& column2) {
  if (column1.name() != column2.name()) {
    std::cout
      << "Column " << columnNumber << " name:" << std::endl
      << "-" << column1.name() << std::endl
      << "+" << column2.name() << std::endl;
    return 1;
  }

  if (column1.physical_type() != column2.physical_type()) {
    std::cout
      << "Column " << columnNumber << " (" << column1.name() << ") physical type:" << std::endl
      << "-" << TypeToString(column1.physical_type()) << std::endl
      << "+" << TypeToString(column2.physical_type()) << std::endl;
    return 1;
  }

  if (!column1.logical_type()->Equals(*(column2.logical_type()))) {
    std::cout
      << "Column " << columnNumber << " (" << column1.name() << ") logical type:" << std::endl
      << "-" << column1.logical_type()->ToString() << std::endl
      << "+" << column2.logical_type()->ToString() << std::endl;
    return 1;
  }

  // To keep concepts simple, let's ignore repetition/definition levels.
  // (We welcome patches that would support def/rep levels....)
  if (column1.max_definition_level() > 1) {
    std::cout
      << "Column " << columnNumber << " (" << column1.name() << ") uses unsupported max_definition_level " << column1.max_definition_level() << std::endl;
    return 2;
  }

  if (column1.max_repetition_level() > 0) {
    std::cout
      << "Column " << columnNumber << " (" << column1.name() << ") uses unsupported max_repetition_level " << column1.max_repetition_level() << std::endl;
    return 2;
  }

  return 0;
}


int diffSchema(const parquet::SchemaDescriptor& schema1, const parquet::SchemaDescriptor& schema2) {
  const int nColumns = schema1.num_columns();
  if (schema2.num_columns() != nColumns) {
    std::cout << "Number of columns:" << std::endl << "-" << nColumns << std::endl << "+" << schema2.num_columns() << std::endl;
    return 1;
  }

  for (int i = 0; i < nColumns; i++) {
    if (diffColumn(i, *(schema1.Column(i)), *(schema2.Column(i)))) {
      return 1;
    }
  }

  return 0;
}


template <typename DType>
void readColumnChunk(int rowGroupNumber, int columnNumber, parquet::TypedColumnReader<DType>& chunk, int nRows, typename DType::c_type* values, uint8_t* valid) {
  std::vector<int16_t> definitionLevels(nRows);
  std::vector<int16_t> repetitionLevels(nRows);
  for (int nRead = 0; nRead < nRows;) {
    int64_t levels_read(0);
    int64_t values_read(0);
    int64_t null_count_out(0);
    int64_t nReadThisBatch = chunk.ReadBatchSpaced(
      nRows - nRead, // batch_size
      &definitionLevels[nRead],
      &repetitionLevels[nRead],
      &values[nRead],
      valid,
      nRead, // valid_bits_offset
      &levels_read,
      &values_read,
      &null_count_out
    );

    if (nReadThisBatch == 0) {
      break;
    }

    nRead += nReadThisBatch;
  }
}


template <typename CType>
std::string valueToString(const CType& v);


template<>
std::string valueToString(const int32_t& i32) {
  return std::to_string(i32);
}


template<>
std::string valueToString(const int64_t& i64) {
  return std::to_string(i64);
}


template<>
std::string valueToString(const parquet::ByteArray& byteArray) {
	return parquet::ByteArrayToString(byteArray);
}


template<>
std::string valueToString(const float& value) {
  static const int kBufferSize = 128; // the number in https://github.com/google/double-conversion/blob/master/test/cctest/test-conversions.cc
  char buf[kBufferSize];
  std::fill(&buf[0], &buf[kBufferSize], 0); // initialize
  double_conversion::StringBuilder builder(buf, kBufferSize);
  const double_conversion::DoubleToStringConverter& converter(double_conversion::DoubleToStringConverter::EcmaScriptConverter());
  if (converter.ToShortestSingle(value, &builder)) {
    // No need to call this->doubleBuilder.Finalize() because we know
    // where the string ends.
    return std::string(&buf[0], builder.position());
  } else {
    return "(error converting float)";
    // I guess we can recover from this. According to the docs, there's no
    // way for this to ever happen anyway.
  }
}


template<>
std::string valueToString(const double& value) {
  static const int kBufferSize = 128; // the number in https://github.com/google/double-conversion/blob/master/test/cctest/test-conversions.cc
  char buf[kBufferSize];
  std::fill(&buf[0], &buf[kBufferSize], 0); // initialize
  double_conversion::StringBuilder builder(&buf[0], kBufferSize);
  const double_conversion::DoubleToStringConverter& converter(double_conversion::DoubleToStringConverter::EcmaScriptConverter());
  if (converter.ToShortest(value, &builder)) {
    // No need to call this->doubleBuilder.Finalize() because we know
    // where the string ends.
    return std::string(&buf[0], builder.position());
  } else {
    return "(error converting double)";
    // I guess we can recover from this. According to the docs, there's no
    // way for this to ever happen anyway.
  }
}


template <typename DType>
int diffColumnChunkTyped(int rowGroupNumber, int columnNumber, parquet::TypedColumnReader<DType>& chunk1, parquet::TypedColumnReader<DType>& chunk2, int nRows) {
  std::vector<typename DType::c_type> values1(nRows);
  std::vector<typename DType::c_type> values2(nRows);
  // `valid_bits`: must "be able to store 1 bit more than required"
  // 0 values => 1 byte
  // 1 value => 1 byte
  // 7 values => 1 byte
  // 8 values => 2 bytes
  // 9 values => 2 bytes
  std::vector<uint8_t> valid1(nRows / 8 + 1); // bitmap
  std::vector<uint8_t> valid2(nRows / 8 + 1); // bitmap

  readColumnChunk(rowGroupNumber, columnNumber, chunk1, nRows, &values1[0], &valid1[0]);
  readColumnChunk(rowGroupNumber, columnNumber, chunk2, nRows, &values2[0], &valid2[0]);

  for (int i = 0; i < nRows; i++) {
    if (arrow::BitUtil::GetBit(&valid1[0], i)) {
      // left: value
      if (arrow::BitUtil::GetBit(&valid2[0], i)) {
        // right: value
        if (values1[i] != values2[i]) {
          std::cout
            << "RowGroup " << rowGroupNumber << ", Column " << columnNumber << ", Row " << i << ":" << std::endl
            << "-" << valueToString(values1[i]) << std::endl
            << "+" << valueToString(values2[i]) << std::endl;
          return 1;
        }
      } else {
        // right: (null)
        std::cout
          << "RowGroup " << rowGroupNumber << ", Column " << columnNumber << ", Row " << i << ":" << std::endl
          << "-" << valueToString(values1[i]) << std::endl
          << "+(null)" << std::endl;
        return 1;
      }
    } else {
      // left: (null)
      if (arrow::BitUtil::GetBit(&valid2[0], i)) {
        // right: value
        std::cout
          << "RowGroup " << rowGroupNumber << ", Column " << columnNumber << ", Row " << i << ":" << std::endl
          << "-(null)" << std::endl
          << "+" << valueToString(values2[i]) << std::endl;
        return 1;
      }
    }
  }

  return 0;
}


int diffColumnChunk(int rowGroupNumber, int columnNumber, parquet::ColumnReader* chunk1, parquet::ColumnReader* chunk2, int nRows) {
#define HANDLE_TYPED(type) \
  { \
    auto chunk1Typed(dynamic_cast<parquet::TypedColumnReader<type>*>(chunk1)); \
    auto chunk2Typed(dynamic_cast<parquet::TypedColumnReader<type>*>(chunk2)); \
    if (chunk1Typed && chunk2Typed) { \
      return diffColumnChunkTyped<type>(rowGroupNumber, columnNumber, *chunk1Typed, *chunk2Typed, nRows); \
    } \
  }
  HANDLE_TYPED(parquet::Int32Type)
  HANDLE_TYPED(parquet::Int64Type)
  HANDLE_TYPED(parquet::FloatType)
  HANDLE_TYPED(parquet::DoubleType)
  HANDLE_TYPED(parquet::ByteArrayType)
#undef HANDLE_TYPED
  std::cout << "Row group " << rowGroupNumber << ", column " << columnNumber << ": unhandled physical data type";
  return 1;
}


int diffRowGroup(int rowGroupNumber, parquet::RowGroupReader& group1, parquet::RowGroupReader& group2) {
  const parquet::RowGroupMetaData* metadata1 = group1.metadata();
  const parquet::RowGroupMetaData* metadata2 = group2.metadata();

  int nRows = metadata1->num_rows();
  if (metadata2->num_rows() != nRows) {
    std::cout
      << "RowGroup " << rowGroupNumber << " number of rows:" << std::endl
      << "-" << nRows << std::endl
      << "+" << metadata2->num_rows() << std::endl;
    return 1;
  }

  // assume both groups have same nColumns (since the caller guarantees it)
  int nColumns = metadata1->num_columns();
  for (int i = 0; i < nColumns; i++) {
    if (diffColumnChunk(rowGroupNumber, i, group1.Column(i).get(), group2.Column(i).get(), nRows)) {
      return 1;
    }
  }

  return 0;
}


/**
 * Return 0 iff both files are equivalent or 1 if they differ.
 *
 * If returning 1, write a difference in text form to std::cout.
 */
int diff(const std::string& path1, const std::string& path2)
{
  std::unique_ptr<parquet::ParquetFileReader> reader1(openParquetFile(path1));
  std::unique_ptr<parquet::ParquetFileReader> reader2(openParquetFile(path2));

  const auto metadata1 = reader1->metadata();
  const auto metadata2 = reader2->metadata();

  if (diffSchema(*(metadata1->schema()), *(metadata2->schema()))) {
    return 1;
  }

  const int nRowGroups = metadata1->num_row_groups();
  if (metadata2->num_row_groups() != nRowGroups) {
    std::cout << "Number of row groups:" << std::endl << "-" << nRowGroups << std::endl << "+" << metadata2->num_row_groups() << std::endl;
    return 1;
  }

  for (int i = 0; i < nRowGroups; i++) {
    if (diffRowGroup(i, *(reader1->RowGroup(i)), *(reader2->RowGroup(i)))) {
      return 1;
    }
  }

  return 0;
}


int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_FILENAME_1 PARQUET_FILENAME_2" << std::endl;
    return 1;
  }

  const std::string path1(argv[1]);
  const std::string path2(argv[2]);

  return diff(path1, path2);
}
