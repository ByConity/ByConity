#pragma once

#include <Common/Logger.h>
#include <Common/config.h>

#if USE_HDFS
#    include <Interpreters/Context.h>
#    include <Storages/DataPart_fwd.h>
#    include <Storages/IStorage.h>
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <Storages/RemoteFile/CnchFileSettings.h>
#    include <Storages/RemoteFile/IStorageCloudFile.h>
#    include <Poco/URI.h>
#    include <common/logger_useful.h>
#    include <common/shared_ptr_helper.h>

namespace DB
{

class StorageCloudHDFS : public shared_ptr_helper<StorageCloudHDFS>, public IStorageCloudFile
{
public:
    class FileBufferClient : public IFileClient
    {
    public:
        FileBufferClient(const ContextPtr & query_context_, const String & hdfs_file_);

        ~FileBufferClient() override = default;

        std::unique_ptr<ReadBuffer> createReadBuffer(const DB::String & file) override;
        std::unique_ptr<WriteBuffer> createWriteBuffer(const DB::String & file) override;

        bool exist(const DB::String & file) override;

        std::string type() override { return "HDFS"; }

        ContextPtr query_context;
        HDFSFSPtr fs;
    };


    LoggerPtr log = getLogger("StorageCloudHDFS");

    ~StorageCloudHDFS() override = default;

    StorageCloudHDFS(
        ContextPtr context_,
        const StorageID & table_id_,
        const ColumnsDescription & required_columns_,
        const ConstraintsDescription & constraints_,
        const FilePartInfos & files_,
        const ASTPtr & setting_changes_,
        const CnchFileArguments & arguments_,
        const CnchFileSettings & settings_)
        : IStorageCloudFile(
            context_,
            table_id_,
            required_columns_,
            constraints_,
            std::make_shared<FileBufferClient>(context_, FileURI(arguments_.url).host_name),
            files_,
            setting_changes_,
            arguments_,
            settings_)
    {
    }
};
}
#endif
