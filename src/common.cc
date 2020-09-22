#include <algorithm>
#include <memory>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "common.h"


std::shared_ptr<arrow::Array> concatenateChunks(const std::vector<std::shared_ptr<arrow::Array>>& chunks)
{
    return ASSERT_ARROW_OK(
        arrow::Concatenate(chunks, arrow::default_memory_pool()),
        "concatenating chunks"
    );
}

std::shared_ptr<arrow::Array> concatenateDictionaryChunks(const std::vector<std::shared_ptr<arrow::Array>>& chunks)
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
            throw new std::runtime_error("concatenateDictionaryChunks() does not handle chunks of varying types");
        }
        std::shared_ptr<arrow::DictionaryArray> dictionaryArray(std::dynamic_pointer_cast<arrow::DictionaryArray>(chunk));
        if (!dictionaryArray) {
            throw new std::runtime_error("concatenateDictionaryChunks() does not handle a non-DictionaryArray chunk");
        }
        if (!dictionaryArray->dictionary()->Equals(dictionary)) {
            throw new std::runtime_error("concatenateDictionaryChunks() does not handle a chunk other than chunk0 having a non-empty dictionary");
        }
        indexChunks.push_back(dictionaryArray->indices());
    }

    std::shared_ptr<arrow::Array> indices = ASSERT_ARROW_OK(
        arrow::Concatenate(indexChunks, arrow::default_memory_pool()),
        "concatenating dictionary index chunks"
    );

    return ASSERT_ARROW_OK(
        arrow::DictionaryArray::FromArrays(type, indices, dictionary),
        "creating DictionaryArray"
    );
}

std::shared_ptr<arrow::Array> chunkedArrayToArray(const arrow::ChunkedArray& input)
{
    if (input.chunks().size() == 0) {
        // Build empty array
        std::shared_ptr<arrow::DataType> type(input.type());
        std::unique_ptr<arrow::ArrayBuilder> builder;
        std::shared_ptr<arrow::Array> output;
        ASSERT_ARROW_OK(
            arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder),
            "make zero-length array builder"
        );
        ASSERT_ARROW_OK(
            builder->Finish(&output),
            "build zero-length array"
        );
        return output;
    } else if (input.chunks().size() == 1) {
        // Input is an array -- exactly what we want
        return input.chunk(0);
    } else {
        // Input has more than one chunk; concatenate them
        // Special-case if it's a dictionary, because [arrow 0.15.0] there is no
        // arrow::Concatenate of dictionary types
        std::shared_ptr<arrow::DictionaryType> dictionaryType(std::dynamic_pointer_cast<arrow::DictionaryType>(input.type()));
        if (dictionaryType) {
            return concatenateDictionaryChunks(input.chunks());
        } else {
            return concatenateChunks(input.chunks());
        }
    }
}

void writeArrowTable(const arrow::Table& arrowTable, const std::string& path)
{
  std::shared_ptr<arrow::io::FileOutputStream> outputStream(ASSERT_ARROW_OK(
      arrow::io::FileOutputStream::Open(path),
      "opening output stream"
  ));
  std::shared_ptr<arrow::ipc::RecordBatchWriter> fileWriter(ASSERT_ARROW_OK(
      arrow::ipc::NewFileWriter(
          outputStream.get(),
          arrowTable.schema(),
          arrow::ipc::IpcWriteOptions {
            .use_threads = false,
            .metadata_version = arrow::ipc::MetadataVersion::V4
          }
      ),
      "opening output file"
  ));
  ASSERT_ARROW_OK(fileWriter->WriteTable(arrowTable), "writing Arrow table");
  ASSERT_ARROW_OK(fileWriter->Close(), "closing Arrow file writer");
  ASSERT_ARROW_OK(outputStream->Close(), "closing Arrow file");
}
