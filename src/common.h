#include <exception>
#include <iostream>
#include <memory>
#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/compute/api.h>


static inline void ASSERT_ARROW_OK(arrow::Status status, const char* message)
{
  if (!status.ok()) {
    std::cerr << "Failure " << message << ": " << status.ToString() << std::endl;
    std::terminate();
  }
}


static arrow::Status chunkedArrayToArray(const arrow::ChunkedArray& input, std::shared_ptr<arrow::Array>* output)
{
  if (input.chunks().size() == 0) {
    std::shared_ptr<arrow::DataType> type(input.type());
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
    if (!status.ok()) {
      return status;
    }
    return builder->Finish(output);
  } else if (input.chunks().size() == 1) {
    *output = input.chunk(0);
    return arrow::Status::OK();
  } else {
    return arrow::Concatenate(input.chunks(), arrow::default_memory_pool(), output);
  }
}


static arrow::Status decodeIfDictionary(std::shared_ptr<arrow::Array>* array) {
  std::shared_ptr<arrow::DictionaryType> dictionaryType = std::dynamic_pointer_cast<arrow::DictionaryType>((*array)->type());
  if (dictionaryType) {
    arrow::compute::FunctionContext ctx;
    return arrow::compute::Cast(&ctx, **array, dictionaryType->value_type(), arrow::compute::CastOptions(true), array);
  } else {
    return arrow::Status::OK();
  }
}
