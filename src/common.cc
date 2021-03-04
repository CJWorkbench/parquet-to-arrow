#include <algorithm>
#include <memory>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "common.h"


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
        return ASSERT_ARROW_OK(
            arrow::Concatenate(input.chunks(), arrow::default_memory_pool()),
            "concatenating chunks"
        );
    }
}

void writeArrowTable(const arrow::Table& arrowTable, const std::string& path)
{
  std::shared_ptr<arrow::io::FileOutputStream> outputStream(ASSERT_ARROW_OK(
      arrow::io::FileOutputStream::Open(path),
      "opening output stream"
  ));
  std::shared_ptr<arrow::ipc::RecordBatchWriter> fileWriter(ASSERT_ARROW_OK(
      arrow::ipc::MakeFileWriter(
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
