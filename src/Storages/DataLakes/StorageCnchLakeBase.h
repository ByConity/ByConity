#pragma once

#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Common/Logger.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
namespace Protos
{
    class LakeScanInfos;
}

struct PrepareContextResult;

class StorageCnchLakeBase : public IStorage, protected WithContext
{
public:
    bool isRemote() const override final { return true; }
    bool supportsOptimizer() const override final { return true; }
    bool supportsDistributedRead() const override final { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportIntermedicateResultCache() const override final { return true; }

    /**
     * For lake table, there are two ways of creating table:
     * 1. Specify each column with name and type.
     * 2. Not specify column, and the storage will generate metadata from lake's schema.
     *
     * And this method is used to check if the metadata is consistent with lake's schema (method 1).
     */
    virtual void checkSchema() const { }

    StorageCnchLakeBase(
        const StorageID & table_id_,
        const String & db_name_,
        const String & table_name_,
        ContextPtr context_,
        std::shared_ptr<CnchHiveSettings> settings_);

    StorageID prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context) override final;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override final;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override final;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override final;

    std::optional<String> getVirtualWarehouseName(VirtualWarehouseType vw_type) const override final;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override final;
    void alter(const AlterCommands & params, ContextPtr local_context, TableLockHolder &) override final;

    virtual void serializeLakeScanInfos(Protos::LakeScanInfos & proto, const LakeScanInfos & lake_scan_infos);
    CnchHiveSettingsPtr getSettings() const { return storage_settings; }

protected:
    virtual size_t maxStreams(ContextPtr local_context) const { return local_context->getSettingsRef().max_threads; }
    virtual PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams)
        = 0;
    void collectResource(ContextPtr local_context, PrepareContextResult & result);

private:
    void checkAlterSettings(const AlterCommands & commands) const;

protected:
    String db_name;
    String table_name;

    CnchHiveSettingsPtr storage_settings;

private:
    LoggerPtr log{getLogger("CnchHive")};
};
}
