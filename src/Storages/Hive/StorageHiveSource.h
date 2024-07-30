#pragma once

#include "Common/config.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Storages/StorageInMemoryMetadata.h"
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
        BlockInfo(const Block & header_, bool need_path_column_, bool need_file_column_, const StorageMetadataPtr & metadata);
        Block getHeader() const;

        Block header;   /// phsical columns + partition columns
        Block to_read;  /// phsical columns
        bool need_path_column = false;
        bool need_file_column = false;

        KeyDescription partition_description;
        std::unordered_map<String, size_t> partition_name_to_index;
        bool all_partition_column = false; /// whether only partition columns present
    };
    using BlockInfoPtr = std::shared_ptr<BlockInfo>;

    struct Allocator
    {
        explicit Allocator(HiveFiles files_);

        /// next file slice to read from
        HiveFilePtr next() const;

        HiveFiles hive_files;
    private:
        mutable std::atomic_size_t unallocated = 0;
    };

    using AllocatorPtr = std::shared_ptr<Allocator>;

    StorageHiveSource(ContextPtr context_, BlockInfoPtr info_, AllocatorPtr allocator_, const std::shared_ptr<SelectQueryInfo> & query_info_);
    ~StorageHiveSource() override;

    Chunk generate() override;
    String getName() const override { return "HiveSource"; }
    void prepareReader();

private:
    Chunk buildResultChunk(Chunk & chunk) const;

    std::shared_ptr<const BlockInfo> block_info;
    std::shared_ptr<const Allocator> allocator;

    HiveFilePtr hive_file;
    SourcePtr data_source;
    std::shared_ptr<IHiveFile::ReadParams> read_params;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    Poco::Logger * log {&Poco::Logger::get("StorageHiveSource")};
};

}

#endif
