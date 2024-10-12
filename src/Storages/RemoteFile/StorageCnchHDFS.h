#pragma once
#include <Common/Logger.h>
#include <Common/config.h>

#if USE_HDFS
#    include <MergeTreeCommon/CnchStorageCommon.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/IStorageCnchFile.h>
#    include <common/shared_ptr_helper.h>

namespace DB
{

class StorageCnchHDFS : public shared_ptr_helper<StorageCnchHDFS>, public IStorageCnchFile
{
public:
    FilePartInfos readFileList(ContextPtr query_context) override;
    void clear(ContextPtr query_context) override;

    /// read hdfs file parts by server local, not send resource to worker
    virtual void readByLocal(
        FileDataPartsCNCHVector parts,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;

    BlockOutputStreamPtr writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;

    ~StorageCnchHDFS() override = default;

private:
    LoggerPtr log = getLogger("StorageCnchHDFS");

public:
    StorageCnchHDFS(
        ContextMutablePtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_)
        : IStorageCnchFile(context_, table_id_, required_columns_, constraints_, setting_changes_, arguments_, settings_)
    {
    }
};
}
#endif
