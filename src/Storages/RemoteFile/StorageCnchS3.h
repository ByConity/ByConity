#pragma once

#include <Common/Logger.h>
#include <Common/config.h>

#if USE_AWS_S3
#    include <IO/S3Common.h>
#    include <MergeTreeCommon/CnchStorageCommon.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/IStorageCnchFile.h>
#    include <common/shared_ptr_helper.h>

namespace DB
{

using S3ClientPtr = std::shared_ptr<Aws::S3::S3Client>;
S3ClientPtr initializeS3Client(const ContextPtr & ctx, const CnchFileArguments & arguments);

class StorageCnchS3 : public shared_ptr_helper<StorageCnchS3>, public IStorageCnchFile
{
public:
    Strings readFileList(ContextPtr query_context) override;

    void clear(ContextPtr query_context) override;

    /// read s3 file by server local, not send resource to worker
    void readByLocal(
        FileDataPartsCNCHVector parts,
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr writeByLocal(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;

    ~StorageCnchS3() override = default;

    S3::URI s3_uri;
    std::shared_ptr<S3::S3Util> s3_util;

private:
    LoggerPtr log = getLogger("StorageCnchS3");

public:
    StorageCnchS3(
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_)
        : IStorageCnchFile(context_, table_id_, required_columns_, constraints_, setting_changes_, arguments_, settings_)
        , s3_uri(arguments_.url)
    {
        if (file_list.size() == 1)
            file_list[0] = s3_uri.key;

        S3ClientPtr client = initializeS3Client(context_, arguments_);
        s3_util = std::make_shared<S3::S3Util>(client, s3_uri.bucket);
    }
};
};
#endif
