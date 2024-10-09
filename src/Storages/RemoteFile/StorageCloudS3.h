#pragma once

#include <Common/Logger.h>
#include <Common/config.h>
#include <IO/S3Common.h>

#if USE_AWS_S3
#    include <Storages/DataPart_fwd.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/IStorageCloudFile.h>
#    include <common/logger_useful.h>
#    include <common/shared_ptr_helper.h>

namespace DB
{
class StorageCloudS3 : public shared_ptr_helper<StorageCloudS3>, public IStorageCloudFile
{
public:
    class FileBufferClient : public IFileClient
    {
    public:
        FileBufferClient(const ContextPtr & query_context_, const std::shared_ptr<S3::S3Util> & s3_util_)
            : context(query_context_), s3_util(s3_util_)
        {
        }

        ~FileBufferClient() override = default;

        std::unique_ptr<ReadBuffer> createReadBuffer(const DB::String & key) override;
        std::unique_ptr<WriteBuffer> createWriteBuffer(const DB::String & key) override;

        bool exist(const DB::String & key) override;
        std::string type() override { return "S3"; }

        ContextPtr context;
        std::shared_ptr<S3::S3Util> s3_util;
    };

    ~StorageCloudS3() override = default;

    std::shared_ptr<S3::S3Util> s3_util;

private:
    LoggerPtr log = getLogger("StorageCloudS3");

public:
    StorageCloudS3(
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const Strings & files,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_,
        const std::shared_ptr<S3::S3Util> & s3_util_)
        : IStorageCloudFile(
            context_,
            table_id_,
            required_columns_,
            constraints_,
            std::make_shared<FileBufferClient>(context_, s3_util_),
            files,
            setting_changes_,
            arguments_,
            settings_)
        , s3_util(s3_util_)
    {
    }
};
}
#endif
