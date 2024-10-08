#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_AWS_S3

#    include <IO/BufferBase.h>
#    include <IO/S3/Credentials.h>
#    include <IO/S3/PocoHTTPClient.h>
#    include <aws/core/Aws.h> // Y_IGNORE
#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/core/auth/AWSCredentialsProviderChain.h>
#    include <aws/core/client/ClientConfiguration.h> // Y_IGNORE
#    include <aws/s3/S3Errors.h>
#    include <aws/s3/model/GetObjectResult.h>
#    include <aws/s3/model/HeadObjectResult.h>
#    include <Poco/URI.h>
#    include <Common/HTTPHeaderEntries.h>
#    include <Common/Logger.h>
#    include <Common/ThreadPool.h>
#    include <common/types.h>
namespace Aws::S3
{
class S3Client;
}

namespace DB
{
class RemoteHostFilter;


bool isS3URIScheme(const String & scheme);
}

namespace DB::S3
{

/// For s3 request exception, contains s3 request's error code
class S3Exception : public Exception
{
public:
    S3Exception(const Aws::S3::S3Error & s3err, const String & extra_msg = "");

    const char * name() const throw() override { return "DB::S3::S3Exception"; }

    Aws::S3::S3Errors getS3ErrorCode() const { return error_type; }

    static String formatS3Error(const Aws::S3::S3Error & err, const String & extra);

    bool isRetryableError() const;

private:
    const char * className() const throw() override { return "DB::S3::S3Exception"; }

    Aws::S3::S3Errors error_type;
};

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::shared_ptr<Aws::S3::S3Client> create(
        std::shared_ptr<Aws::Client::ClientConfiguration> client_configuration,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        const String & server_side_encryption_customer_key_base64,
        HTTPHeaderEntries headers,
        CredentialsConfiguration credential_config,
        const String & session_token = {});

    std::shared_ptr<Aws::Client::ClientConfiguration> createCRTHttpClientConfiguration();

    std::shared_ptr<Aws::Client::ClientConfiguration> createClientConfiguration(
        const String & force_region,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects,
        uint32_t http_keep_alive_timeout_ms,
        size_t http_connection_pool_size,
        bool wait_on_pool_size_limit);

private:
    ClientFactory();

    Aws::SDKOptions aws_options;
};

/**
 * Represents S3 URI.
 *
 * The following patterns are allowed:
 * s3://bucket/key
 * http(s)://endpoint/bucket/key
 */
struct URI
{
    Poco::URI uri;
    // Custom endpoint if URI scheme is not S3.
    String region;
    String endpoint;
    String bucket;
    String key;
    String storage_name;

    bool is_virtual_hosted_style;

    URI() = default;
    explicit URI(const Poco::URI & uri_, bool parse_region_ = false);
    explicit URI(const std::string & uri_, bool parse_region_ = false) : URI(Poco::URI(uri_), parse_region_) { }

    String toString() const
    {
        return fmt::format(
            "`storage_name = {}, region = {}, url = {}/{}/{}, is_virtual_hosted_style = {}`",
            storage_name,
            region,
            endpoint,
            bucket,
            key,
            is_virtual_hosted_style);
    }

    static void validateBucket(const String & bucket, const Poco::URI & uri);
    static bool isS3Scheme(const Poco::URI & uri);
    static bool isS3Scheme(const String & scheme);
};

class S3Config
{
public:
    explicit S3Config(const String & ini_file_path);

    S3Config(
        const String & endpoint_,
        const String & region_,
        const String & bucket_,
        const String & ak_id_,
        const String & ak_secret_,
        const String & root_prefix_,
        const String & session_token_ = "",
        bool is_virtual_hosted_style_ = false,
        int connect_timeout_ms_ = 10000,
        int request_timeout_ms_ = 30000,
        int max_redirects_ = 10,
        int max_connections_ = 100,
        uint32_t http_keep_alive_timeout_ms_ = 5000,
        size_t http_connection_pool_size_ = 1024,
        size_t slow_read_ms_ = 100,
        UInt64 min_upload_part_size_ = 16 * 1024 * 1024,
        UInt64 max_single_part_upload_size_ = 16 * 1024 * 1024)
        : max_redirects(max_redirects_)
        , connect_timeout_ms(connect_timeout_ms_)
        , request_timeout_ms(request_timeout_ms_)
        , max_connections(max_connections_)
        , endpoint(endpoint_)
        , region(region_)
        , bucket(bucket_)
        , root_prefix(root_prefix_)
        , ak_id(ak_id_)
        , ak_secret(ak_secret_)
        , session_token(session_token_)
        , is_virtual_hosted_style(is_virtual_hosted_style_)
        , http_keep_alive_timeout_ms(http_keep_alive_timeout_ms_)
        , http_connection_pool_size(http_connection_pool_size_)
        , slow_read_ms(slow_read_ms_)
        , min_upload_part_size(min_upload_part_size_)
        , max_single_part_upload_size(max_single_part_upload_size_)
    {
    }

    S3Config(const Poco::Util::AbstractConfiguration & cfg, const String & cfg_prefix);

    void collectCredentialsFromEnv();

    std::shared_ptr<Aws::S3::S3Client> create() const;

    int max_redirects;
    int connect_timeout_ms;
    int request_timeout_ms;
    int max_connections;

    String endpoint;
    String region;
    String bucket;
    String root_prefix;

    String ak_id;
    String ak_secret;
    String session_token;
    CredentialsConfiguration credential_config;

    bool is_virtual_hosted_style;
    uint32_t http_keep_alive_timeout_ms;
    size_t http_connection_pool_size;
    size_t slow_read_ms{100};
    UInt64 min_upload_part_size;
    UInt64 max_single_part_upload_size;
};

class S3Util
{
public:
    S3Util(const std::shared_ptr<Aws::S3::S3Client> & client_, const String & bucket_, bool for_disk_s3_ = false)
        : client(client_), bucket(bucket_), for_disk_s3(for_disk_s3_)
    {
    }

    // Access object metadata
    // NOTE(wsy) Interface using head method won't throw exact exception
    size_t getObjectSize(const String & key) const;
    std::map<String, String> getObjectMeta(const String & key) const;
    bool exists(const String & key) const;

    // Read object
    bool read(const String & key, size_t offset, size_t size, BufferBase::Buffer & buffer) const;

    struct S3ListResult
    {
        bool has_more{true};
        std::optional<String> token;
        Strings object_names;
        std::vector<size_t> object_sizes;
        std::vector<bool> is_common_prefix;
    };
    S3ListResult listObjectsWithDelimiter(const String & prefix, String delimiter = "/", bool include_delimiter = false) const;
    S3ListResult listObjectsWithPrefix(const String & prefix, const std::optional<String> & token, int limit = 1000) const;

    // Write object
    String createMultipartUpload(
        const String & key,
        const std::optional<std::map<String, String>> & meta = std::nullopt,
        const std::optional<std::map<String, String>> & tags = std::nullopt) const;
    void completeMultipartUpload(const String & key, const String & upload_id, const std::vector<String> & etags) const;
    void abortMultipartUpload(const String & key, const String & upload_id) const;
    String uploadPart(
        const String & key,
        const String & upload_id,
        size_t part_number,
        size_t size,
        const std::shared_ptr<Aws::StringStream> & stream) const;

    void upload(
        const String & key,
        size_t size,
        const std::shared_ptr<Aws::StringStream> & stream,
        const std::optional<std::map<String, String>> & metadata = std::nullopt,
        const std::optional<std::map<String, String>> & tags = std::nullopt) const;

    void copyObject(const String & from_key, const String & to_bucket, const String & to_key);

    // Delete object
    void deleteObject(const String & key, bool check_existence = true) const;
    void deleteObjects(const std::vector<String> & keys) const;
    void deleteObjectsInBatch(const std::vector<String> & key, size_t batch_size = 1000) const;
    void deleteObjectsWithPrefix(
        const String & prefix, const std::function<bool(const S3Util &, const String &)> & filter, size_t batch_size = 1000) const;

    // Internal client and info
    const std::shared_ptr<Aws::S3::S3Client> & getClient() const { return client; }
    const String & getBucket() const { return bucket; }

private:
    static String urlEncodeMap(const std::map<String, String> & mp);

    Aws::S3::Model::HeadObjectResult headObject(const String & key) const;
    Aws::S3::Model::GetObjectResult headObjectByGet(const String & key) const;

    std::shared_ptr<Aws::S3::S3Client> client;
    const String bucket;

    bool for_disk_s3 = false; // to choose which profile events should be incremented
};

constexpr auto S3_DEFAULT_BATCH_CLEAN_SIZE = 1000;

class S3LazyCleaner
{
public:
    S3LazyCleaner(
        const S3::S3Util & s3_util_,
        const std::function<bool(const S3::S3Util &, const String &)> & filter_,
        size_t max_threads_,
        size_t batch_clean_size_ = S3_DEFAULT_BATCH_CLEAN_SIZE);
    ~S3LazyCleaner() noexcept;

    void push(const String & key_);
    void finalize();

private:
    void lazyRemove(const std::optional<String> & key_);

    LoggerPtr logger;

    size_t batch_clean_size;
    std::function<bool(const S3::S3Util &, const String &)> filter;
    S3::S3Util s3_util;

    ExceptionHandler except_hdl;
    std::unique_ptr<ThreadPool> clean_pool;

    std::mutex remove_keys_mu;
    std::vector<String> keys_to_remove;
};

struct AuthSettings
{
    static AuthSettings loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config);

    std::string access_key_id;
    std::string access_key_secret;
    std::string session_token;
    std::string region;
    std::string server_side_encryption_customer_key_base64;

    HTTPHeaderEntries headers;

    std::optional<bool> use_environment_credentials;
    std::optional<bool> use_insecure_imds_request;
    std::optional<uint64_t> expiration_window_seconds;
    std::optional<bool> no_sign_request;

    bool operator==(const AuthSettings & other) const
    {
        return access_key_id == other.access_key_id && access_key_secret == other.access_key_secret && region == other.region
            && server_side_encryption_customer_key_base64 == other.server_side_encryption_customer_key_base64 && headers == other.headers
            && use_environment_credentials == other.use_environment_credentials
            && use_insecure_imds_request == other.use_insecure_imds_request;
    }

    void updateFrom(const AuthSettings & from);
};

/// return whether the exception worth retry or not
bool processReadException(Exception & e, LoggerPtr log, const String & bucket, const String & key, size_t read_offset, size_t attempt);

void resetSessionIfNeeded(bool read_all_range_successfully, std::optional<Aws::S3::Model::GetObjectResult> & read_result);

}

#endif
