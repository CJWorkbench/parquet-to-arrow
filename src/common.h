#include <cstdlib>
#include <iostream>
#include <memory>
#include <arrow/api.h>


static inline void ASSERT_ARROW_OK(arrow::Status status, const char* message)
{
  if (!status.ok()) {
    std::cerr << "Failure " << message << ": " << status.ToString() << std::endl;
    std::_Exit(1);
  }
}


arrow::Status decodeIfDictionary(std::shared_ptr<arrow::Array>* array);
arrow::Status chunkedArrayToArray(const arrow::ChunkedArray& input, std::shared_ptr<arrow::Array>* output);
void writeArrowTable(const arrow::Table& arrowTable, const std::string& path);
