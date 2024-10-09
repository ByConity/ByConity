#pragma once

#include <Common/Logger.h>
#include <Common/config.h>
#if USE_HIVE

#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Storages/DataLakes/StorageCnchLakeBase.h>
#include <Storages/Hive/HiveFile/IHiveFile_fwd.h>
#include <Storages/Hive/Metastore/IMetaClient.h>
#include <common/shared_ptr_helper.h>

namespace Apache::Hadoop::Hive
{
    class Table;
    class TableStatsResult;
}
namespace ApacheHive = Apache::Hadoop::Hive;

namespace DB
{
namespace Protos
{
    class ProtoHiveFiles;
}

struct PrepareContextResult;
struct HivePartition;
class IMetaClient;
class HiveWhereOptimizer;
class IDirectoryLister;

class StorageCnchHive : public shared_ptr_helper<StorageCnchHive>, public StorageCnchLakeBase
{
public:
    friend class IDirectoryLister;

    std::string getName() const override { return "CnchHive"; }

    void startup() override;

    StorageCnchHive(
        const StorageID & table_id_,
        const String & hive_metastore_url_,
        const String & hive_db_name_,
        const String & hive_table_name_,
        std::optional<StorageInMemoryMetadata> metadata_,
        ContextPtr context_,
        IMetaClientPtr meta_client,
        std::shared_ptr<CnchHiveSettings> settings_);

    NamesAndTypesList getVirtuals() const override;
    ASTPtr applyFilter(ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr, PlanNodeStatisticsPtr) const override;

    void setHiveMetaClient(const IMetaClientPtr & client);
    void initialize(StorageInMemoryMetadata metadata_);

    std::optional<TableStatistics> getTableStats(const Strings & columns, ContextPtr local_context) override;
    std::vector<std::pair<String, UInt64>> getPartitionLastModificationTime(const StorageMetadataPtr & metadata_snapshot, bool binary_format = true);

protected:
    size_t maxStreams(ContextPtr local_context) const override;

    PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams) override;

    HivePartitions selectPartitions(
        ContextPtr local_context,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info);

    std::optional<UInt64> getSelectedBucketNumber(
        ContextPtr local_context,
        SelectQueryInfo & query_info,
        const StorageMetadataPtr & metadata_snapshot) const;

    /// DirectoryList is not multi-threaded
    virtual std::shared_ptr<IDirectoryLister> getDirectoryLister(ContextPtr local_context);

    String hive_metastore_url;

    std::shared_ptr<IMetaClient> hive_client;
    std::shared_ptr<ApacheHive::Table> hive_table;
    std::exception_ptr hive_exception = nullptr;

private:
    LoggerPtr log {getLogger("CnchHive")};

};
}

#endif
