#pragma once
#include <Common/Logger.h>
#include "Processors/Chunk.h"
#include "config_formats.h"
#if USE_ORC

#    include <Core/Names.h>
#    include <arrow/io/type_fwd.h>
#    include <orc/Common.hh>
#    include <orc/OrcFile.hh>
#    include "Columns/IColumn.h"
#    include "Storages/IStorage.h"
#    include "Storages/MergeTree/KeyCondition.h"
namespace DB
{
struct ColumnSize; // defined in IStorage
DataTypePtr parseORCType(const orc::Type * orc_type, bool skip_columns_with_unsupported_types, bool & skipped);

// merge io
class IOMergeBuffer
{
public:
    struct IORange
    {
        IORange(const int64_t offset_, const int64_t size_, const bool is_active_ = true)
            : offset(offset_), size(size_), is_active(is_active_)
        {
        }
        int64_t offset;
        int64_t size;
        bool is_active = true;
        bool operator<(const IORange & x) const { return offset < x.offset; }
    };
    struct CoalesceOptions
    {
        static constexpr int64_t MB = 1024 * 1024;
        int64_t max_dist_size = 1 * MB;
        int64_t max_buffer_size = 8 * MB;
    };
    struct SharedBuffer
    {
        // request range
        int64_t raw_offset;
        int64_t raw_size;
        // request range after alignment
        int64_t offset;
        int64_t size;
        int64_t ref_count;
        std::vector<uint8_t> buffer;
        void align(int64_t align_size, int64_t file_size);
        String toString();
    };

    IOMergeBuffer(std::shared_ptr<arrow::io::RandomAccessFile> random_file, std::string filename, size_t file_size);
    ~IOMergeBuffer() = default;
    // arrow::Result<int64_t> read(void* data, int64_t count);
    arrow::Status readAtFully(int64_t offset, void * out, int64_t count);
    arrow::Result<SharedBuffer *> findSharedBuffer(size_t offset, size_t count);
    arrow::Status setIORanges(const std::vector<IORange> & ranges, bool coalesce_lazy_column = true);
    void release();
    void setCoalesceOptions(const CoalesceOptions & options_) { options = options_; }

private:
    // arrow::Status getBytes(const uint8_t** buffer, size_t offset, size_t nbytes); // get pointer in sharedBuffer
    arrow::Status sortAndCheckOverlap(std::vector<IORange> & ranges);
    void mergeSmallRanges(const std::vector<IORange> & ranges);
    std::shared_ptr<arrow::io::RandomAccessFile> random_file;
    std::string filename;
    std::map<int64_t, SharedBuffer> buffer_map;
    CoalesceOptions options;
    int64_t file_size = 0;
    int64_t align_size = 0;
};


class ORCArrowInputStream : public orc::InputStream
{
public:
    explicit ORCArrowInputStream(const std::shared_ptr<arrow::io::RandomAccessFile> & file);

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void * buf, uint64_t length, uint64_t offset) override;

    const std::string & getName() const override;

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file;
};


/// Thread safe input stream
class CachedORCArrowInputStream : public orc::InputStream
{
public:
    explicit CachedORCArrowInputStream(const std::shared_ptr<arrow::io::RandomAccessFile> & file);

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    uint64_t getNaturalReadSizeAfterSeek() const override;
    void read(void * buf, uint64_t length, uint64_t offset) override;
    void prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length) override;
    const std::string & getName() const override;
    void setIORanges(std::vector<orc::InputStream::IORange> & io_ranges) override;
    void clearIORanges() override;
    virtual bool isIORangesEnabled() const override { return true; }

private:
    bool isAlreadyCachedInBuffer(uint64_t offset, uint64_t length);

    void readDirect(void * buf, uint64_t length, uint64_t offset);

    std::shared_ptr<arrow::io::RandomAccessFile> file;
    std::unique_ptr<IOMergeBuffer> merge_buffer;
    std::vector<char> cached_buffer;
    size_t cached_offset = 0;
};
std::unique_ptr<orc::SearchArgument> buildORCSearchArgument(
    const KeyCondition & key_condition, const Block & header, const orc::Type & schema, const FormatSettings & format_settings);


class ORCColumnToCHColumn
{
public:
    using ORCColumnPtr = const orc::ColumnVectorBatch *;
    using ORCTypePtr = const orc::Type *;
    using ORCColumnWithType = std::pair<ORCColumnPtr, ORCTypePtr>;
    using NameToColumnPtr = std::unordered_map<std::string, ORCColumnWithType>;

    ORCColumnToCHColumn(
        const Block & header_,
        bool allow_missing_columns_,
        bool null_as_default_,
        bool case_insensitive_matching_ = false,
        bool allow_out_of_range_ = false);

    void orcTableToCHChunk(
        Chunk & res,
        const orc::Type * schema,
        const orc::ColumnVectorBatch * table,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr) const;

    /// almost the same as orcTableToChChunk, with a specical purose for 2-stage read.
    /// It will ignore the schema check as we did in orcTableToChChunk.
    void orcTableToCHChunkWithFields(
        const Block & local_header,
        Chunk & res,
        const orc::Type * schema,
        const orc::ColumnVectorBatch * table,
        const std::vector<int> & fields,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr) const;

    void orcColumnsToCHChunk(
        Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values = nullptr) const;

    void orcColumnsToCHChunk(
        const Block & local_header,
        Chunk & res,
        NameToColumnPtr & name_to_column_ptr,
        size_t num_rows,
        BlockMissingValues * block_missing_values = nullptr) const;

private:
    const Block & header;
    /// If false, throw exception if some columns in header not exists in arrow table.
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;
    bool allow_out_of_range;
    NameSet header_columns;
    LoggerPtr logger = getLogger("ORCColumnToCHColumn");
};

IStorage::ColumnSizeByName getOrcColumnsSize(orc::Reader & orc_reader);
}
#endif
