#pragma once

#include <Common/Logger.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
struct PrepareContextResult;

class IStorageCnchFile : public IStorage,
                         public WithMutableContext,
                         public CnchStorageCommonHelper
{
public:
    IStorageCnchFile(
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_);

    ~IStorageCnchFile() override = default;

    virtual Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    virtual void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    virtual Strings readFileList(ContextPtr query_context) = 0;

    virtual void clear(ContextPtr query_context) = 0;

    /// read remote file parts by server local, not send resource to worker
    virtual void readByLocal(
        FileDataPartsCNCHVector parts,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
        = 0;

    PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context);

    virtual BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;

    virtual BlockOutputStreamPtr writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context);

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const override;

    void alter(const AlterCommands & commands, ContextPtr query_context, TableLockHolder & /*table_lock_holder*/) override;

    virtual String getName() const override { return "CnchFile"; }

    virtual bool isRemote() const override { return true; }

    virtual NamesAndTypesList getVirtuals() const override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr query_context, QueryProcessingStage::Enum stage, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const override;
    
    bool supportsOptimizer() const override { return true; }
    bool supportsDistributedRead() const override { return true; }
    StorageID prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context) override;

private:
    Strings getPrunedFiles(const ContextPtr & query_context, const ASTPtr & query);

    void collectResource(const ContextPtr & query_context, const FileDataPartsCNCHVector & parts, const String & local_table_name);

    void checkAlterSettings(const AlterCommands & commands) const;

public:
    CnchFileArguments arguments;
    CnchFileSettings settings;

    Strings file_list;
    NamesAndTypesList virtual_columns;
    Block virtual_header;

private:
    LoggerPtr log = getLogger("StorageCnchFile");
};

}
