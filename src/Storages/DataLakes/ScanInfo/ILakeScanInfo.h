#pragma once

#include <Formats/SharedParsingThreadPool.h>
#include <IO/ReadSettings.h>
#include <Processors/ISource.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <common/types.h>

namespace DB
{
namespace Protos
{
    class LakeScanInfo;
    class LakeScanInfos;
}

class ILakeScanInfo;
using LakeScanInfoPtr = std::shared_ptr<ILakeScanInfo>;
using LakeScanInfos = std::vector<LakeScanInfoPtr>;

/*
 * ILakeScanInfo is an abstraction used in Data Lake scenarios where data from a single data source needs to be scanned in a distributed manner.
 * It represents the smallest unit of distribution for scanning tasks. This interface ensures that the scanning process can be efficiently
 * divided and assigned across multiple nodes or processes, enabling scalable and parallel data processing within the Data Lake environment.
 */
class ILakeScanInfo
{
public:
    enum StorageType
    {
        Hive = 0,
        Hudi = 1,
        Paimon = 2,
    };

    struct ReadParams
    {
        size_t max_block_size;
        FormatSettings format_settings;
        ContextPtr context;
        std::optional<size_t> slice;
        ReadSettings read_settings;
        std::shared_ptr<SelectQueryInfo> query_info;
        SharedParsingThreadPoolPtr shared_pool;
    };

    /**
     * Create a reader for current scan info.
     */
    virtual SourcePtr getReader(const Block & block, const std::shared_ptr<ReadParams> & read_params) = 0;

    /**
     * Get the identifier of the scan info, which maybe used as one of following purposes:
     * 1. map key
     * 2. cache key for intermediate result
     * 3. used to calculate worker id when there is no distribution_id
     */
    virtual String identifier() const = 0;

    /**
     * These following methods is used to determine which worker should process this scan info.
     * The distribution_id is used to determine the worker index directly, otherwise the hash value
     * if identifier() will be used to calculate the worker index.
     */
    void setDistributionId(size_t distribution_id_) { distribution_id = distribution_id_; }
    size_t calWorkerIdx(size_t worker_num);

    /**
     * Serialize the scan info to a protobuf message.
     * Each implementation should override this method to serialize its own fields,
     * and call it's direct base class method to serialize common fields.
     */
    virtual void serialize(Protos::LakeScanInfo & proto) const;

    std::optional<String> getPartitionId() const { return partition_id; }
    std::optional<size_t> getDistributionId() { return distribution_id; }

    static LakeScanInfoPtr deserialize(
        const Protos::LakeScanInfo & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);

    static LakeScanInfos deserialize(
        const Protos::LakeScanInfos & proto,
        const ContextPtr & context,
        const StorageMetadataPtr & metadata,
        const CnchHiveSettings & settings);

    const StorageType storage_type;

protected:
    explicit ILakeScanInfo(StorageType storage_type_);
    virtual ~ILakeScanInfo() = default;

    static String md5(const String & content);

    // For some storage type, like hive and hudi, partition column wil be manually added to
    // final chunk (see LakeUnifiedSource::generatePostProcessor), so we need to storage
    // partition id in scan info.
    std::optional<String> partition_id;

    // If distribution_id is set, it will be used to determine the worker index directly.
    std::optional<size_t> distribution_id;
};

}
