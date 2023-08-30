#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Processors/QueryPipeline.h"
#include <Processors/Sources/SourceWithProgress.h>
#include "Storages/Hive/HiveFile/IHiveFile.h"

namespace DB
{
class PullingPipelineExecutor;

class StorageHiveSource : public SourceWithProgress, WithContext
{
public:
    /// shared between threads
    struct BlockInfo
    {
        BlockInfo() = delete;
        BlockInfo(const Block & header_, bool need_path_column_, bool need_file_column_, KeyDescription partition_);
        Block getHeader() const;

        Block header;   /// phsical columns + partition columns
        Block to_read;  /// phsical columns
        bool need_path_column = false;
        bool need_file_column = false;

        KeyDescription partition_description;
        std::vector<size_t> partition_column_idx;
    };
    using BlockInfoPtr = std::shared_ptr<BlockInfo>;

    struct FileSlice
    {
        int file {-1};
        int slice {0};

        bool empty() const { return file == -1; }
        void reset() { file = -1; }
    };
    struct Allocator
    {
        explicit Allocator(HiveFiles files_);
        size_t size() const { return files.size(); }

        /// next file slice to read from
        void next(FileSlice & file_slice) const;

        HiveFiles files;
        bool allow_allocate_by_slice = true;

    private:
        mutable std::atomic_int unallocated = 0;
        bool nextSlice(FileSlice & file_slice) const;
        mutable std::vector<std::atomic_int> slice_progress;
    };

    using AllocatorPtr = std::shared_ptr<Allocator>;

    StorageHiveSource(ContextPtr context_, BlockInfoPtr info_, AllocatorPtr allocator_);
    ~StorageHiveSource() override;

    Chunk generate() override;
    String getName() const override { return "HiveSource"; }
    void prepareReader();

private:
    void buildResultChunk(Chunk & chunk) const;

    bool need_partition_columns = true;
    FileSlice current_file_slice;
    std::shared_ptr<const BlockInfo> block_info;
    std::shared_ptr<const Allocator> allocator;

    SourcePtr data_source;
    std::shared_ptr<IHiveFile::ReadParams> read_params;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    Poco::Logger * log {&Poco::Logger::get("StorageHiveSource")};
};

}

#endif
