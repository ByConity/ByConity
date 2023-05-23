#pragma once

#include "Common/config.h"

#if USE_JAVA_EXTENSIONS
#include "Storages/DataLakes/HudiPartition.h"
#include "Processors/Sources/SourceWithProgress.h"
#include "Storages/DataLakes/IStorageDataLake.h"

#include <hudi.pb.h>

namespace DB
{
namespace Protos { class HudiTable; }
class JNIMetaClient;
class PullingPipelineExecutor;

using HudiTableProperties = std::unordered_map<String, String>;

class StorageHudiSource : public SourceWithProgress, WithContext
{
public:
    struct SourceInfo
    {
        size_t max_block_size;
        bool need_path_column = false;
        Names phsical_columns;

        RemoteFileInfos remote_files;
        mutable std::atomic_int idx = 0;

        RemoteFileInfo const * next() const;
    };
    using SourceInfoPtr = std::shared_ptr<SourceInfo>;
    using SourceInfoConstPtr = std::shared_ptr<const SourceInfo>;

    StorageHudiSource(Block header_, const SourceInfoPtr & info_, const HudiTableProperties & properties_, Poco::Logger *log_);

    static Block getHeader(Block header, const SourceInfo & source_info);
    void resetReader();
    String getName() const override;
    Chunk generate() override;

private:
    Block header;
    SourceInfoConstPtr source_info;
    HudiTableProperties properties;
    Poco::Logger * log;

    RemoteFileInfo const * current_file;
    SourcePtr source;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
};

class StorageHudi : public shared_ptr_helper<StorageHudi>, public IStorageDataLake
{
public:
    String getName() const override { return "Hudi"; }

    StorageHudi(const StorageID & table_id_,
                const String & base_path,
                ContextPtr context_);

    Strings getRequiredPartitions(const StorageInMemoryMetadata & metadata, SelectQueryInfo & query_info);

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

private:
    void buildProperties(const Protos::HudiTable & table);

    String base_path;
    HudiTableProperties table_properties;
    std::shared_ptr<JNIMetaClient> jni_client;
    Poco::Logger * log{&Poco::Logger::get("StorageHudi")};
};

}

#endif
