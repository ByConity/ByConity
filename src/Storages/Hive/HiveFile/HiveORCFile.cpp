#include "Storages/Hive/HiveFile/HiveORCFile.h"
#if USE_HIVE

#include "Processors/Formats/Impl/ArrowBufferedStreams.h"
#include "Processors/Formats/Impl/LMNativeORCBlockInputFormat.h"
#include "Processors/Formats/Impl/ORCBlockInputFormat.h"
#include "IO/ReadSettings.h"

#include <arrow/adapters/orc/adapter.h>
#include <orc/Statistics.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (const ::arrow::Status & _s = (status); !_s.ok())           \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

HiveORCFile::HiveORCFile() = default;
HiveORCFile::~HiveORCFile() = default;

std::optional<size_t> HiveORCFile::numRows()
{
    if (num_rows)
        return *num_rows;

    auto seekable_buffer = readFile(ReadSettings{});
    std::atomic_int stopped = false;
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;

    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(
        asArrowFile(*seekable_buffer, FormatSettings{}, stopped, "ORC", ORC_MAGIC_BYTES, /* avoid_buffering */ true),
        arrow::default_memory_pool())
    .Value(&file_reader));
    num_rows = file_reader->NumberOfRows();
    return *num_rows;
}



};

#endif
