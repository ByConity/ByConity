#pragma once
#include <Common/Logger.h>
#include <mutex>
#include <orc/Reader.hh>
#include "Processors/Formats/Impl/OrcChunkReader.h"
#include "config_formats.h"
#if USE_ORC

#    include <Formats/FormatSettings.h>
#    include <IO/ReadBufferFromString.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <arrow/io/type_fwd.h>
#    include <orc/Common.hh>
#    include <orc/OrcFile.hh>
#    include "Columns/IColumn.h"
#    include "OrcCommon.h"
#    include "Processors/Formats/Impl/ParallelDecodingBlockInputFormat.h"

namespace arrow::io
{
class RandomAccessFile;
}

namespace DB
{
class KeyCondition;
class LMNativeORCBlockInputFormat : public ParallelDecodingBlockInputFormat
{
public:
    LMNativeORCBlockInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        const FormatSettings & format_settings_,
        size_t max_download_threads,
        size_t max_parsing_threads,
        SharedParsingThreadPoolPtr parsing_thread_pool);

    ~LMNativeORCBlockInputFormat() override;
    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;
    void setQueryInfo(const SelectQueryInfo & query_info, ContextPtr context) override;
    IStorage::ColumnSizeByName getColumnSizes() override;
    bool supportsPrewhere() const override { return true; }

protected:
    void initializeFileReader() override;
    void initializeRowGroupReaderIfNeeded(size_t row_group_idx) override;
    size_t getNumberOfRowGroups() override;
    void resetRowGroupReader(size_t row_group_idx) override;

    std::optional<PendingChunk> readBatch(size_t row_group_idx) override;
    size_t getRowCount() override;

    std::unique_ptr<orc::Reader> orc_file_reader = nullptr; // used for reading orc file meta.
    ScanParams scan_params;
    std::vector<std::unique_ptr<OrcScanner>> scanners;
    std::vector<std::unique_ptr<std::once_flag>> init_scanners_once;
    LoggerPtr log = getLogger("LMNativeORCBlockInputFormat");
};


}

#endif
