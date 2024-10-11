#pragma once

#include <Common/Logger.h>
#include <Storages/DataPart_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <Poco/URI.h>
#include <Common/config.h>
#include <common/logger_useful.h>
#include <common/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class IStorageCloudFile : public IStorage, public WithMutableContext
{
public:
    IStorageCloudFile(
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const FileClientPtr & client_,
        Strings files_,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_);

    ~IStorageCloudFile() override = default;

    virtual Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    virtual void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    virtual BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    virtual String getName() const override { return "CloudFile"; }

    virtual NamesAndTypesList getVirtuals() const override;

    virtual void loadDataParts(FileDataPartsCNCHVector & file_parts);

    FileClientPtr client;
    Strings file_list;

    CnchFileArguments arguments;
    CnchFileSettings settings;

    NamesAndTypesList virtual_columns;
    FileDataPartsCNCHVector parts{};

private:
    LoggerPtr log = getLogger("IStorageCloudFile");
};

}
