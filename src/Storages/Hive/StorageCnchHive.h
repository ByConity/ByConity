#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "MergeTreeCommon/CnchStorageCommon.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include <common/shared_ptr_helper.h>

namespace Apache::Hadoop::Hive
{
    class Table;
}

namespace DB
{
struct PrepareContextResult;
struct HivePartition;
class IMetaClient;
class HiveWhereOptimizer;

class StorageCnchHive : public shared_ptr_helper<StorageCnchHive>, public IStorage, WithContext
{
public:
    std::string getName() const override { return "CnchHive"; }
    bool isRemote() const override { return true; }
    bool isBucketTable() const override;

    StorageCnchHive(
        const StorageID & table_id_,
        const String & hive_metastore_url_,
        const String & hive_db_name_,
        const String & hive_table_name_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        std::shared_ptr<CnchHiveSettings> settings_);

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    std::optional<String> getVirtualWarehouseName(VirtualWarehouseType vw_type) const override;

    PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams);

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

private:

    void collectResource(ContextPtr local_context, PrepareContextResult & result);

    HivePartitions selectPartitions(
        ContextPtr local_context,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const HiveWhereOptimizer & optimizer);

    HiveFiles selectFilesFromPartitions(
        ContextPtr local_context,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const HiveWhereOptimizer & optimizer,
        const HivePartitions & partitions);

    std::set<Int64> selectBucketNumbers(
        ContextPtr local_context,
        const StorageMetadataPtr & metdata_snapshot,
        const HiveWhereOptimizer & optimizer);

    String hive_metastore_url;
    String hive_db_name;
    String hive_table_name;

    std::shared_ptr<IMetaClient> hive_client;
    std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table;

    std::shared_ptr<CnchHiveSettings> storage_settings;
    Poco::Logger * log {&Poco::Logger::get("CnchHive")};
};
}

#endif
