#pragma once

#include <cstddef>
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
#include "common/types.h"
#include <Common/RemoteHostFilter.h>
#include <Common/SettingsChanges.h>
#include <Storages/RemoteFile/CnchFileSettings.h>
#include <common/logger_useful.h>


namespace DB
{

struct FilePartInfo
{
public:
    explicit FilePartInfo(String name_) : name(std::move(name_)) { }
    explicit FilePartInfo(String name_, size_t size_) : name(std::move(name_)), size(size_) { }

    String getBasicPartName() const { return name; }

    String name;
    size_t size;
};

using FilePartInfos = std::vector<FilePartInfo>;

class FileDataPart
{
public:
    explicit FileDataPart(const String & file_) : info(file_) { }
    explicit FileDataPart(const FilePartInfo & file_info_) : info(file_info_) { }

    FilePartInfo info;
};

class FilesIterator
{
public:
    explicit FilesIterator(const FileDataPartsCNCHVector & parts_) : parts(parts_), iterator(parts.begin()) { }
    const FilePartInfo * next()
    {
        std::lock_guard lock(mutex);
        if (iterator == parts.end())
            return nullptr;
        auto file = *iterator;
        ++iterator;
        return &file->info;
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
}
