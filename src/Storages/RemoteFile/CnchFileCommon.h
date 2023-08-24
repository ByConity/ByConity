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
    HeaderCollection headers_from_ast{};

    bool updated{false};

    explicit StorageS3Configuration(
        const String & url_,
        const S3::AuthSettings & auth_settings_ = {},
        const S3Settings::ReadWriteSettings & rw_settings_ = {},
        const HeaderCollection & headers_from_ast_ = {})
        : uri(S3::URI(url_)), auth_settings(auth_settings_), rw_settings(rw_settings_), headers_from_ast(headers_from_ast_)
    {
    }

    void updateS3Client(const ContextPtr & ctx, const CnchFileArguments & arguments)
    {
        if (uri.bucket.find_first_of("*?{") != DB::String::npos)
            throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto s3_settings = ctx->getStorageS3Settings().getSettings(uri.endpoint);
        s3_settings.rw_settings.updateFromSettingsIfEmpty(ctx->getSettingsRef());

        auth_settings = s3_settings.auth_settings;
        rw_settings = s3_settings.rw_settings;
        use_read_ahead = ctx->getSettingsRef().s3_use_read_ahead;

        if (!ctx->getSettingsRef().s3_access_key_id.value.empty())
            auth_settings.access_key_id = ctx->getSettingsRef().s3_access_key_id;
        else if (!arguments.access_key_id.empty())
            auth_settings.access_key_id = arguments.access_key_id;

        if (!ctx->getSettingsRef().s3_access_key_secret.value.empty())
            auth_settings.access_key_secret = ctx->getSettingsRef().s3_access_key_secret;
        else if (!arguments.access_key_secret.empty())
            auth_settings.access_key_secret = arguments.access_key_secret;

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region, ctx->getRemoteHostFilter(), static_cast<unsigned>(ctx->getSettingsRef().s3_max_redirects));

        client_configuration.endpointOverride = uri.endpoint.empty() ? s3_settings.endpoint : uri.endpoint;
        client_configuration.maxConnections = static_cast<unsigned>(rw_settings.max_connections);

        auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.access_key_secret);
        auto headers = auth_settings.headers;
        if (!headers_from_ast.empty())
            headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

        client = S3::ClientFactory::instance().create(
            client_configuration,
            uri.is_virtual_hosted_style,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            auth_settings.server_side_encryption_customer_key_base64,
            std::move(headers),
            auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
            auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));

        LOG_DEBUG(
            &Poco::Logger::get("StorageS3Configuration"),
            fmt::format(
                "update s3 client, config: {}, region = {}, endpoint = {}, bucket = {}, key = {}, ak/sk = {} -> {}, use_read_ahead = {}",
                uri.toString(),
                auth_settings.region,
                uri.endpoint,
                uri.bucket,
                uri.key,
                auth_settings.access_key_id,
                auth_settings.access_key_secret,
                use_read_ahead));
    }
};


}
