#pragma once

#include <utility>
#include <Core/Defines.h>
//#include <Core/SettingsCommon.h>
#include <Core/Types.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <IO/S3Common.h>
#include <Storages/DataPart_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>
#include <aws/core/auth/AWSCredentials.h>
#include <Poco/URI.h>
#include <Common/RemoteHostFilter.h>
#include <Common/SettingsChanges.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <common/logger_useful.h>


namespace DB
{

struct FilePartInfo
{
public:
    explicit FilePartInfo(String name_) : name(std::move(name_)){}

    String getBasicPartName() const { return name; }

    String name;
};

class FileDataPart
{
public:
    explicit FileDataPart(const String & file_) : info(file_) { }

    FilePartInfo info;
};

class FilesIterator
{
public:
    explicit FilesIterator(const FileDataPartsCNCHVector & parts_) : parts(parts_), iterator(parts.begin()) { }
    String next()
    {
        std::lock_guard lock(mutex);
        if (iterator == parts.end())
            return "";
        auto file = *iterator;
        ++iterator;
        return file->info.name;
    }

private:
    std::mutex mutex;
    FileDataPartsCNCHVector parts{};
    FileDataPartsCNCHVector::iterator iterator{};
};

struct FileURI
{
    explicit FileURI(const String & uri)
    {
        auto host_pos = uri.find("//");
        auto path_pos = std::string::npos;

        if (host_pos == std::string::npos)
        {
            host_name = "/"; // use config host
            path_pos = 0;
        }
        else
        {
            path_pos = uri.find('/', host_pos + 2);
            host_name = uri.substr(0, path_pos) + "/";
        }

        auto file_name_pos = uri.find_last_of('/');
        if (file_name_pos == std::string::npos)
            throw Exception(
                "Storage CnchHDFS/CnchS3 requires valid URL to be set, should be include `/`", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        dir_path = uri.substr(path_pos, file_name_pos) + "/";
        file_path = uri.substr(path_pos);
        file_name = uri.substr(file_name_pos + 1);
    }

    /// host_name/dir_path/file_name, file_path = dir_path/file_name
    String host_name;
    String dir_path;
    String file_path;
    String file_name;
};
using HDFSURI = FileURI;

class IFileClient
{
public:
    virtual std::unique_ptr<ReadBuffer> createReadBuffer(const String & file) = 0;
    virtual std::unique_ptr<WriteBuffer> createWriteBuffer(const String & file) = 0;

    virtual bool exist(const String & file) = 0;

    virtual ~IFileClient() = default;

    virtual std::string type() { return "unknow";}
};

using FileClientPtr = std::shared_ptr<IFileClient>;


struct CnchFileArguments
{
    String url;
    String format_name;
    String structure;
    String compression_method{"auto"};

    bool is_glob_path{false};
    bool is_function_table{false};

    String access_key_id;
    String access_key_secret;

    ASTPtr partition_by;
};

using S3ClientPtr = std::shared_ptr<Aws::S3::S3Client>;
struct StorageS3Configuration
{
    S3::URI uri;
    S3ClientPtr client;
    bool use_read_ahead{true};

    S3::AuthSettings auth_settings;
    S3Settings::ReadWriteSettings rw_settings;

    /// Headers from ast is a part of static configuration.
    HTTPHeaderEntries headers_from_ast{};

    bool updated{false};

    explicit StorageS3Configuration(
        const String & url_,
        const S3::AuthSettings & auth_settings_ = {},
        const S3Settings::ReadWriteSettings & rw_settings_ = {},
        const HTTPHeaderEntries & headers_from_ast_ = {})
        : uri(S3::URI(url_)), auth_settings(auth_settings_), rw_settings(rw_settings_), headers_from_ast(headers_from_ast_)
    {
    }

    void updateS3Client(const ContextPtr & ctx, const CnchFileArguments & arguments);
};


}
