#include <algorithm>
#include <memory>
#include <arrow/array/concatenate.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "common.h"


arrow::Status concatenateChunks(const std::vector<std::shared_ptr<arrow::Array>>& chunks, std::shared_ptr<arrow::Array>* output)
{
    return arrow::Concatenate(chunks, arrow::default_memory_pool(), output);
}

arrow::Status concatenateDictionaryChunks(const std::vector<std::shared_ptr<arrow::Array>>& chunks, std::shared_ptr<arrow::Array>* output)
{
    std::shared_ptr<arrow::DictionaryArray> chunk0(std::dynamic_pointer_cast<arrow::DictionaryArray>(chunks[0]));
    if (!chunk0) {
        throw new std::runtime_error("concatenateDictionaryChunks() chunk0 is not a DictionaryArray");
    }
    std::shared_ptr<arrow::DataType> type(chunk0->type());
    std::shared_ptr<arrow::Array> dictionary(chunk0->dictionary());

    std::vector<std::shared_ptr<arrow::Array>> indexChunks;
    for (const auto chunk : chunks) {
        const std::shared_ptr<arrow::DataType> chunkType(chunk->type());
        if (!type->Equals(chunkType)) {
            return arrow::Status::NotImplemented("chunks of varying types");
        }
        std::shared_ptr<arrow::DictionaryArray> dictionaryArray(std::dynamic_pointer_cast<arrow::DictionaryArray>(chunk));
        if (!dictionaryArray) {
            return arrow::Status::NotImplemented("at least one chunk is not a DictionaryArray");
        }
        if (!dictionaryArray->dictionary()->Equals(dictionary)) {
            return arrow::Status::NotImplemented("a chunk other thank chunk0 had a non-empty dictionary");
        }
        indexChunks.push_back(dictionaryArray->indices());
    }

    std::shared_ptr<arrow::Array> indices;
    ASSERT_ARROW_OK(
        arrow::Concatenate(indexChunks, arrow::default_memory_pool(), &indices),
        "concatenating dictionary index chunks"
    );

    return arrow::DictionaryArray::FromArrays(type, indices, dictionary, output);
}

arrow::Status chunkedArrayToArray(const arrow::ChunkedArray& input, std::shared_ptr<arrow::Array>* output)
{
    if (input.chunks().size() == 0) {
        // Build empty array
        std::shared_ptr<arrow::DataType> type(input.type());
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto status = arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder);
        if (!status.ok()) {
            return status;
        }
        return builder->Finish(output);
    } else if (input.chunks().size() == 1) {
        // Input is an array -- exactly what we want
        *output = input.chunk(0);
        return arrow::Status::OK();
    } else {
        // Input has more than one chunk; concatenate them
        // Special-case if it's a dictionary, because [arrow 0.15.0] there is no
        // arrow::Concatenate of dictionary types
        std::shared_ptr<arrow::DictionaryType> dictionaryType(std::dynamic_pointer_cast<arrow::DictionaryType>(input.type()));
        if (dictionaryType) {
            return concatenateDictionaryChunks(input.chunks(), output);
        } else {
            return concatenateChunks(input.chunks(), output);
        }
    }
}


arrow::Status decodeIfDictionary(std::shared_ptr<arrow::Array>* array)
{
    std::shared_ptr<arrow::DictionaryType> dictionaryType = std::dynamic_pointer_cast<arrow::DictionaryType>((*array)->type());
    if (dictionaryType) {
        arrow::compute::FunctionContext ctx;
        return arrow::compute::Cast(&ctx, **array, dictionaryType->value_type(), arrow::compute::CastOptions(true), array);
    } else {
        return arrow::Status::OK();
    }
}

void writeArrowTable(const arrow::Table& arrowTable, const std::string& path)
{
  std::shared_ptr<arrow::io::FileOutputStream> outputStream(ASSERT_ARROW_OK(
    arrow::io::FileOutputStream::Open(path),
    "opening output stream"
  ));
  std::shared_ptr<arrow::ipc::RecordBatchWriter> fileWriter;
  ASSERT_ARROW_OK(arrow::ipc::RecordBatchFileWriter::Open(outputStream.get(), arrowTable.schema(), &fileWriter), "creating file writer");
  ASSERT_ARROW_OK(fileWriter->WriteTable(arrowTable), "writing Arrow table");
  ASSERT_ARROW_OK(fileWriter->Close(), "closing Arrow file");
}
