#pragma once

#include <unordered_map>
#include <Formats/SharedParsingThreadPool.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/DataLakes/ScanInfo/ILakeScanInfo.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Logger.h>

namespace DB
{
class PullingPipelineExecutor;

class LakeUnifiedSource : public SourceWithProgress, WithContext, public shared_ptr_helper<LakeUnifiedSource>
{
public:
    /// shared between threads
    struct BlockInfo
    {
        BlockInfo() = delete;
        BlockInfo(const Block & header_, const StorageMetadataPtr & metadata_);
        Block getHeader() const;

        Block header; /// physical columns + partition columns + virtual columns
        Block physical_header; /// physical columns

        StorageMetadataPtr metadata;
        std::unordered_map<String, size_t> partition_name_to_index;
    };
    using BlockInfoPtr = std::shared_ptr<BlockInfo>;

    struct Allocator
    {
        explicit Allocator(LakeScanInfos && lake_scan_infos_);

        /// next file slice to read from
        LakeScanInfoPtr next() const;

        LakeScanInfos lake_scan_infos;

    private:
        mutable std::atomic_size_t unallocated = 0;
    };

    using AllocatorPtr = std::shared_ptr<Allocator>;

    LakeUnifiedSource(
        ContextPtr context_,
        size_t max_block_size_,
        BlockInfoPtr info_,
        AllocatorPtr allocator_,
        const std::shared_ptr<SelectQueryInfo> & query_info_,
        const SharedParsingThreadPoolPtr & shared_pool_);

    ~LakeUnifiedSource() override;

    String getName() const override { return "LakeUnifiedSource"; }
    Chunk generate() override;
    void onFinish() override { shared_pool->finishStream(); }
    void prepareReader();

private:
    void resetReader();
    SourcePtr getSource() const;
    Chunk generatePostProcessor(Chunk & chunk);
    Chunk generatePostProcessorForHive(Chunk & chunk);

    HivePartitionPtr getHivePartition(const String & partition_id);

    std::shared_ptr<const BlockInfo> block_info;
    std::shared_ptr<const Allocator> allocator;

    LakeScanInfoPtr lake_scan_info;
    SourcePtr data_source;
    SharedParsingThreadPoolPtr shared_pool;
    const bool need_only_count;

    std::shared_ptr<ILakeScanInfo::ReadParams> read_params;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;

    // Only used for hive
    // partition_id -> partition
    std::unordered_map<String, HivePartitionPtr> hive_partitions;

    LoggerPtr log{getLogger("LakeUnifiedSource")};
};

}
