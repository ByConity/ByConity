#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/Metastore/IMetaClient.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "MergeTreeCommon/CnchStorageCommon.h"
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

class StorageCnchHive : public shared_ptr_helper<StorageCnchHive>, public IStorage, protected WithContext
{
public:
    friend class IDirectoryLister;

    std::string getName() const override { return "CnchHive"; }
    bool isRemote() const override { return true; }

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

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    std::optional<String> getVirtualWarehouseName(VirtualWarehouseType vw_type) const override;

    virtual PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    ASTPtr applyFilter(ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr, PlanNodeStatisticsPtr) const override;

    void setHiveMetaClient(const IMetaClientPtr & client);
    void initialize(StorageInMemoryMetadata metadata_);

    NamesAndTypesList getVirtuals() const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void alter(const AlterCommands & params, ContextPtr local_context, TableLockHolder &) override;

    bool supportsOptimizer() const override { return true; }
    bool supportsDistributedRead() const override { return true; }
    StorageID prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context) override;
    std::optional<TableStatistics> getTableStats(const Strings & columns, ContextPtr local_context) override;
    std::vector<std::pair<String, UInt64>> getPartitionLastModificationTime(const StorageMetadataPtr & metadata_snapshot, bool binary_format = true);

    virtual void serializeHiveFiles(Protos::ProtoHiveFiles & proto, const HiveFiles & hive_files);
    bool supportIntermedicateResultCache() const override { return true; }

protected:
    void collectResource(ContextPtr local_context, PrepareContextResult & result);

    HivePartitions selectPartitions(
        ContextPtr local_context,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info);

    std::optional<UInt64> getSelectedBucketNumber(
        ContextPtr local_context,
        SelectQueryInfo & query_info,
        const StorageMetadataPtr & metadata_snapshot) const;

    /// DirectoryList is not multi-threaded
    virtual std::shared_ptr<IDirectoryLister> getDirectoryLister();

    void checkAlterSettings(const AlterCommands & commands) const;

    String hive_metastore_url;
    String hive_db_name;
    String hive_table_name;

    std::shared_ptr<IMetaClient> hive_client;
    std::shared_ptr<ApacheHive::Table> hive_table;
    std::shared_ptr<CnchHiveSettings> storage_settings;
    std::exception_ptr hive_exception = nullptr;

private:
    Poco::Logger * log {&Poco::Logger::get("CnchHive")};

};
}

#endif
