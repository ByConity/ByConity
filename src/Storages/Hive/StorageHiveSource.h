#pragma once

#include <Common/Logger.h>
#include <unordered_map>
#include "Common/config.h"
#if USE_HIVE

#include <Processors/Sources/SourceWithProgress.h>
#include "Formats/SharedParsingThreadPool.h"
#include "Processors/QueryPipeline.h"
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Storages/StorageInMemoryMetadata.h"

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
        BlockInfo(const Block & header_, const StorageMetadataPtr & metadata_);
        Block getHeader() const;

        Block header;           /// physical columns + partition columns + virtual columns
        Block physical_header;  /// physical columns

        StorageMetadataPtr metadata;
        std::unordered_map<String, size_t> partition_name_to_index;
    };
    using BlockInfoPtr = std::shared_ptr<BlockInfo>;

    struct Allocator
    {
        explicit Allocator(HiveFiles && files_);

        /// next file slice to read from
        HiveFilePtr next() const;

        HiveFiles hive_files;
    private:
        mutable std::atomic_size_t unallocated = 0;
    };

    using AllocatorPtr = std::shared_ptr<Allocator>;

    StorageHiveSource(
        ContextPtr context_,
        size_t max_block_size,
        BlockInfoPtr info_,
        AllocatorPtr allocator_,
        const std::shared_ptr<SelectQueryInfo> & query_info_,
        const SharedParsingThreadPoolPtr & shared_pool_);

    ~StorageHiveSource() override;

    Chunk generate() override;
    String getName() const override { return "HiveSource"; }
    void onFinish() override { shared_pool->finishStream(); }
    void prepareReader();

private:
    Chunk buildResultChunk(Chunk & chunk) const;

    std::shared_ptr<const BlockInfo> block_info;
    std::shared_ptr<const Allocator> allocator;

    HiveFilePtr hive_file;
    SourcePtr data_source;
    SharedParsingThreadPoolPtr shared_pool;
    const bool need_only_count;

    std::shared_ptr<IHiveFile::ReadParams> read_params;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    LoggerPtr log {getLogger("StorageHiveSource")};
};

}

#endif
