#include <memory>
#include <IO/HTTPCommon.h>
#include <IO/S3Common.h>
#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromString.h>
#    include <Storages/StorageS3Settings.h>
#    include <boost/program_options.hpp>
#    include <Common/quoteString.h>
#    include <Common/getNumberOfPhysicalCPUCores.h>

#    include <aws/core/Version.h>
#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/auth/AWSCredentialsProviderChain.h>
#    include <aws/core/auth/STSCredentialsProvider.h>
#    include <aws/core/client/DefaultRetryStrategy.h>
#    include <aws/core/http/HttpClientFactory.h>
#    include <aws/core/platform/Environment.h>
#    include <aws/core/platform/OSVersionInfo.h>
#    include <aws/core/utils/HashingUtils.h>
#    include <aws/core/utils/json/JsonSerializer.h>
#    include <aws/core/utils/logging/LogMacros.h>
#    include <aws/core/utils/logging/LogSystemInterface.h>
#    include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/AbortMultipartUploadRequest.h>
#    include <aws/s3/model/CompleteMultipartUploadRequest.h>
#    include <aws/s3/model/CreateMultipartUploadRequest.h>
#    include <aws/s3/model/DeleteObjectRequest.h>
#    include <aws/s3/model/DeleteObjectResult.h>
#    include <aws/s3/model/DeleteObjectsRequest.h>
#    include <aws/s3/model/DeleteObjectsResult.h>
#    include <aws/s3/model/GetObjectRequest.h>
#    include <aws/s3/model/GetObjectTaggingRequest.h>
#    include <aws/s3/model/GetObjectTaggingResult.h>
#    include <aws/s3/model/HeadObjectRequest.h>
#    include <aws/s3/model/HeadObjectResult.h>
#    include <aws/s3/model/ListObjectsV2Request.h>
#    include <aws/s3/model/ListObjectsV2Result.h>
#    include <aws/s3/model/PutObjectRequest.h>
#    include <aws/s3/model/UploadPartRequest.h>

#    include <IO/S3/PocoHTTPClient.h>
#    include <IO/S3/PocoHTTPClientFactory.h>
#    include <IO/S3/CustomCRTHttpClientFactory.h>
#    include <IO/S3/CustomCRTHttpClient.h>
#    include <IO/S3/AWSOptionsConfig.h>
#    include <IO/S3/SessionAwareIOStream.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include <re2/re2.h>
#    include <Poco/URI.h>
#    include <Poco/Util/AbstractConfiguration.h>
#    include <common/logger_useful.h>
#    include <common/types.h>
#    include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event S3ReadRequestsErrors;
    extern const Event S3WriteRequestsErrors;

    extern const Event S3ResetSessions;
    extern const Event S3PreservedSessions;

    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
    extern const Event S3HeadObject;
    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3AbortMultipartUpload;
    extern const Event S3UploadPart;
    extern const Event S3PutObject;
    extern const Event S3GetObject;

    extern const Event DiskS3HeadObject;
    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3AbortMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3PutObject;
}

namespace
{
DB::PooledHTTPSessionPtr getSession(Aws::S3::Model::GetObjectResult & read_result)
{
    if (auto * session_aware_stream = dynamic_cast<DB::S3::SessionAwareIOStream<DB::PooledHTTPSessionPtr> *>(&read_result.GetBody()))
        return static_cast<DB::PooledHTTPSessionPtr &>(session_aware_stream->getSession());

    return {};
}
}

namespace DB::S3::Auth
{
const char * S3_LOGGER_TAG_NAMES[][2] = {
    {"AWSClient", "AWSClient"},
    {"AWSAuthV4Signer", "AWSClient (AWSAuthV4Signer)"},
};

const std::pair<DB::LogsLevel, Poco::Message::Priority> & convertLogLevel(Aws::Utils::Logging::LogLevel log_level)
{
    static const std::unordered_map<Aws::Utils::Logging::LogLevel, std::pair<DB::LogsLevel, Poco::Message::Priority>> mapping = {
        {Aws::Utils::Logging::LogLevel::Off, {DB::LogsLevel::none, Poco::Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Fatal, {DB::LogsLevel::error, Poco::Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Error, {DB::LogsLevel::error, Poco::Message::PRIO_ERROR}},
        {Aws::Utils::Logging::LogLevel::Warn, {DB::LogsLevel::warning, Poco::Message::PRIO_WARNING}},
        {Aws::Utils::Logging::LogLevel::Info, {DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION}},
        {Aws::Utils::Logging::LogLevel::Debug, {DB::LogsLevel::trace, Poco::Message::PRIO_TRACE}},
        {Aws::Utils::Logging::LogLevel::Trace, {DB::LogsLevel::trace, Poco::Message::PRIO_TRACE}},
    };
    return mapping.at(log_level);
}

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    AWSLogger(Aws::Utils::Logging::LogLevel log_level)
    {
        for (auto [tag, name] : S3_LOGGER_TAG_NAMES)
            tag_loggers[tag] = &Poco::Logger::get(name);

        default_logger = tag_loggers[S3_LOGGER_TAG_NAMES[0][0]];
        log_level_ = log_level;
    }

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return log_level_; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        callLogImpl(log_level, tag, format_str); /// FIXME. Variadic arguments?
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        callLogImpl(log_level, tag, message_stream.str().c_str());
    }

    void callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message)
    {
        const auto & [level, prio] = convertLogLevel(log_level);
        if (tag_loggers.count(tag) > 0)
        {
            LOG_IMPL(tag_loggers[tag], level, prio, "{}", message);
        }
        else
        {
            LOG_IMPL(default_logger, level, prio, "{}: {}", tag, message);
        }
    }

    void Flush() final { }

private:
    Poco::Logger * default_logger;
    std::unordered_map<String, Poco::Logger *> tag_loggers;
    Aws::Utils::Logging::LogLevel log_level_;
};
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int S3_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

bool isS3URIScheme(const String& scheme) {
    return (strcasecmp(scheme.c_str(), "s3") == 0) || (strcasecmp(scheme.c_str(), "http") == 0) || (strcasecmp(scheme.c_str(), "https") == 0);
}

namespace S3
{
    S3Exception::S3Exception(const Aws::S3::S3Error & s3err, const String & extra_msg)
        : Exception(formatS3Error(s3err, extra_msg), ErrorCodes::S3_ERROR)
        , error_type(s3err.GetErrorType())
    {
    }

    String S3Exception::formatS3Error(const Aws::S3::S3Error & err, const String & extra)
    {
        return fmt::format(
            "Encounter exception when request s3, HTTP Code: {}, "
            "RemoteHost: {}, RequestID: {}, ExceptionName: {}, ErrorMessage: {}, Extra: {}",
            err.GetResponseCode(),
            err.GetRemoteHostIpAddress(),
            err.GetRequestId(),
            err.GetExceptionName(),
            err.GetMessage(),
            extra);
    }

    bool S3Exception::isRetryableError() const
    {
        /// Looks like these list is quite conservative, add more codes if you wish
        static const std::unordered_set<Aws::S3::S3Errors> unretryable_errors = {
            Aws::S3::S3Errors::NO_SUCH_KEY,
            Aws::S3::S3Errors::ACCESS_DENIED,
            Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID,
            Aws::S3::S3Errors::INVALID_SIGNATURE,
            Aws::S3::S3Errors::NO_SUCH_UPLOAD,
            Aws::S3::S3Errors::NO_SUCH_BUCKET,
        };

        return !unretryable_errors.contains(error_type);
    }


    ClientFactory::ClientFactory()
    {
        // used for initializing aws API when creating client factory
        AWSOptionsConfig& aws_options_config = AWSOptionsConfig::instance();
        aws_options = Aws::SDKOptions{};
        aws_options.loggingOptions.logLevel = aws_options_config.convertStringToLogLevel(aws_options_config.log_level);
        aws_options.ioOptions.clientBootstrap_create_fn = [&aws_options_config]() {
            Aws::Crt::Io::EventLoopGroup eventLoopGroup(aws_options_config.aws_event_loop_size);
            Aws::Crt::Io::DefaultHostResolver defaultHostResolver(eventLoopGroup,
                aws_options_config.max_hosts, aws_options_config.max_TTL);
            auto clientBootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>("CrtClientBootstrap", eventLoopGroup, defaultHostResolver);
            clientBootstrap->EnableBlockingShutdown();
            return clientBootstrap;
        };
        Aws::InitAPI(aws_options);
        Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<Auth::AWSLogger>(
            aws_options.loggingOptions.logLevel));
        if (aws_options_config.use_crt_http_client) {
            Aws::Http::SetHttpClientFactory(std::make_shared<CustomCRTHttpClientFactory>());
        } else {
            Aws::Http::SetHttpClientFactory(std::make_shared<PocoHTTPClientFactory>());
        }
    }

    ClientFactory::~ClientFactory()
    {
        Aws::Utils::Logging::ShutdownAWSLogging();
        Aws::ShutdownAPI(aws_options);
    }

    ClientFactory & ClientFactory::instance()
    {
        static ClientFactory ret;
        return ret;
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        std::shared_ptr<Aws::Client::ClientConfiguration> client_configuration,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        const String & server_side_encryption_customer_key_base64,
        HTTPHeaderEntries headers,
        CredentialsConfiguration credential_config,
        const String & session_token)
    {
        if (!server_side_encryption_customer_key_base64.empty())
        {
            /// See S3Client::GeneratePresignedUrlWithSSEC().
            headers.push_back(
                {Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                 Aws::S3::Model::ServerSideEncryptionMapper::GetNameForServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256)});

            headers.push_back({Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, server_side_encryption_customer_key_base64});

            Aws::Utils::ByteBuffer buffer = Aws::Utils::HashingUtils::Base64Decode(server_side_encryption_customer_key_base64);
            String str_buffer(reinterpret_cast<char *>(buffer.GetUnderlyingData()), buffer.GetLength());
            headers.push_back(
                {Aws::S3::SSEHeaders::SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                 Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(str_buffer))});
        }

        // These will be added after request signing
        if (AWSOptionsConfig::instance().use_crt_http_client) {
            std::shared_ptr<CustomCRTHttpClientConfiguration> crt_configuration =
                std::static_pointer_cast<CustomCRTHttpClientConfiguration>(client_configuration);
            crt_configuration->extra_headers = std::move(headers);
        } else {
            std::shared_ptr<PocoHTTPClientConfiguration> poco_configuration =
                std::static_pointer_cast<PocoHTTPClientConfiguration>(client_configuration);
            poco_configuration->extra_headers = std::move(headers);
        }

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key, session_token);
        auto credentials_provider = std::make_shared<S3CredentialsProviderChain>(
            client_configuration, std::move(credentials),
            credential_config);

        return std::make_shared<Aws::S3::S3Client>(
            credentials_provider,
            *client_configuration, // Client configuration.
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            is_virtual_hosted_style || client_configuration->endpointOverride.empty() // Use virtual addressing only if endpoint is not specified.
        );
    }

    std::shared_ptr<Aws::Client::ClientConfiguration> ClientFactory::createCRTHttpClientConfiguration() {
        return std::make_shared<CustomCRTHttpClientConfiguration>();
    }

    std::shared_ptr<Aws::Client::ClientConfiguration> ClientFactory::createClientConfiguration( // NOLINT
        const String & force_region,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects,
        uint32_t http_keep_alive_timeout_ms,
        size_t http_connection_pool_size,
        bool wait_on_pool_size_limit)
    {
        if (AWSOptionsConfig::instance().use_crt_http_client) {
            return std::make_shared<CustomCRTHttpClientConfiguration>();
        } else {
            return std::make_shared<PocoHTTPClientConfiguration>(force_region, remote_host_filter,
                s3_max_redirects, http_keep_alive_timeout_ms, http_connection_pool_size,
                wait_on_pool_size_limit);
        }
    }

    URI::URI(const Poco::URI & uri_, bool parse_region)
    {
        /// Case when bucket name represented in domain name of S3 URL.
        /// E.g. (https://bucket-name.s3.Region.amazonaws.com/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access

        static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3|cos|tos)([.\-][a-z0-9\-.:]+))");


        /// Case when bucket name and key represented in path of S3 URL.
        /// E.g. (https://s3.Region.amazonaws.com/bucket-name/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
        static const RE2 path_style_pattern("^/([^/]*)/(.*)");

        static constexpr auto S3 = "S3";
        static constexpr auto COSN = "COSN";
        static constexpr auto COS = "COS";
        static constexpr auto TOS = "TOS";


        uri = uri_;
        storage_name = S3;

        if (uri.getHost().empty())
            throw Exception("Host is empty in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        if (isS3Scheme(uri.getScheme()))
        {
            // URI has format s3://bucket/key
            endpoint = "";
            bucket = uri.getAuthority();
            validateBucket(bucket, uri);
            if (uri.getPath().length() <= 1)
                throw Exception("Invalid S3 URI: no key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
            key = uri.getPath().substr(1);
            is_virtual_hosted_style = false;
            return;
        }

        String name;
        String endpoint_authority_from_uri;

        if (re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
        {
            is_virtual_hosted_style = true;
            endpoint = uri.getScheme() + "://" + name + endpoint_authority_from_uri;

            /// S3 specification requires at least 3 and at most 63 characters in bucket name.
            /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
            if (bucket.length() < 3 || bucket.length() > 63)
                throw Exception("Bucket name length is out of bounds in virtual hosted style S3 URI: "
                    + quoteString(bucket) + "(" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);

            if (!uri.getPath().empty())
            {
                /// Remove leading '/' from path to extract key.
                key = uri.getPath().substr(1);
            }

            boost::to_upper(name);

            if (name != S3 && name != COS && name != TOS)
            {
                throw Exception("Object storage system name is unrecognized in virtual hosted style S3 URI: "
                    + quoteString(name) + "(" + uri.toString() + ")",
                    ErrorCodes::BAD_ARGUMENTS);
            }
            if (name == S3)
            {
                storage_name = name;
            }
            else if (name == TOS)
            {
                storage_name = TOS;
            }
            else
            {
                storage_name = COSN;
            }
        }
        else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
        {
            is_virtual_hosted_style = false;
            endpoint = uri.getScheme() + "://" + uri.getAuthority();

            validateBucket(bucket, uri);
        }
        else
            throw Exception("Bucket or key name are invalid in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        if (!parse_region)
            return;

        // try parse region
        std::vector<String> endpoint_splices;
        boost::split(endpoint_splices, endpoint, boost::is_any_of("."));
        if (endpoint_splices.empty())
            return;

        if (storage_name == COSN || storage_name == S3)
        {
            if (endpoint_splices.size() < 2)
                return;
            region = endpoint_splices[1];
        }
        else if (storage_name == TOS)
        {
            if (endpoint_splices.size() < 1)
                return;
            region = endpoint_splices[0].starts_with("tos-s3") ? endpoint_splices[0].substr(7) : endpoint_splices[0].substr(4);
        }

    }

    void URI::validateBucket(const String & bucket, const Poco::URI & uri)
    {
        /// S3 specification requires at least 3 and at most 63 characters in bucket name.
        /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
        if (bucket.length() < 3 || bucket.length() > 63)
            throw Exception("Bucket name length is out of bounds in virtual hosted style S3 URI:" + quoteString(bucket)
                + (!uri.empty() ? " (" + uri.toString() + ")" : ""), ErrorCodes::BAD_ARGUMENTS);
    }

    S3Config::S3Config(const String & ini_file_path)
    {
        namespace po = boost::program_options;
        po::options_description s3_opts("s3_opts");

        s3_opts.add_options()
            ("s3.max_redirects", po::value<int>(&max_redirects)->default_value(10)->implicit_value(10), "max_redirects")
            ("s3.connect_timeout_ms", po::value<int>(&connect_timeout_ms)->default_value(30000)->implicit_value(30000), "connect timeout ms")
            ("s3.request_timeout_ms", po::value<int>(&request_timeout_ms)->default_value(30000)->implicit_value(30000), "request timeout ms")
            ("s3.max_connections", po::value<int>(&max_connections)->default_value(1024)->implicit_value(1024), "max connections")
            ("s3.region", po::value<String>(&region)->default_value("")->implicit_value(""), "region")
            ("s3.endpoint", po::value<String>(&endpoint)->required(), "endpoint")
            ("s3.bucket", po::value<String>(&bucket)->required(), "bucket")
            ("s3.ak_id", po::value<String>(&ak_id)->required(), "ak id")
            ("s3.ak_secret", po::value<String>(&ak_secret)->required(), "ak secret")
            ("s3.root_prefix", po::value<String>(&root_prefix)->required(), "root prefix")
            ("s3.is_virtual_hosted_style", po::value<bool>(&is_virtual_hosted_style)->default_value(false)->implicit_value(false), "is virtual hosted style or not")
            ("s3.http_keep_alive_timeout_ms", po::value<uint32_t>(&http_keep_alive_timeout_ms)->default_value(5000)->implicit_value(5000), "http keep alive time")
            ("s3.http_connection_pool_size", po::value<size_t>(&http_connection_pool_size)->implicit_value(1024)->default_value(1024), "http pool size")
            ("s3.min_upload_part_size", po::value<UInt64>(&min_upload_part_size)->implicit_value(16 * 1024 * 1024)->default_value(16 * 1024 * 1024), "min upload part size")
            ("s3.max_single_part_upload_size", po::value<UInt64>(&max_single_part_upload_size)->implicit_value(16 * 1024 * 1024)->default_value(16 * 1024 * 1024), "max single part upload size");

        po::parsed_options opts = po::parse_config_file(ini_file_path.c_str(), s3_opts);
        po::variables_map vm;
        po::store(opts, vm);
        po::notify(vm);

        if (root_prefix.empty() || root_prefix[0] == '/')
            throw Exception("Root prefix can't be empty or start with '/'", ErrorCodes::BAD_ARGUMENTS);
    }

    S3Config::S3Config(const Poco::Util::AbstractConfiguration & cfg, const String & cfg_prefix)
    {
        // Attention! Client factory will be initialized once when adding the first s3 disk
        const String aws_options_prefix = "aws_options";
        AWSOptionsConfig& aws_options_config = AWSOptionsConfig::instance();
        aws_options_config.use_crt_http_client = cfg.getBool(aws_options_prefix + ".use_crt_http_client", false);
        aws_options_config.aws_event_loop_size = cfg.getInt(aws_options_prefix + ".aws_event_loop_size", getNumberOfPhysicalCPUCores());
        aws_options_config.max_hosts = cfg.getInt(aws_options_prefix + ".max_hosts", 32);
        aws_options_config.max_TTL = cfg.getInt(aws_options_prefix + ".max_TTL", 300);
        aws_options_config.log_level = cfg.getString(aws_options_prefix + ".log_level", "Off");

        max_redirects = cfg.getInt(cfg_prefix + ".max_redirects", 10);
        connect_timeout_ms = cfg.getInt(cfg_prefix + ".connect_timeout_ms", 10000);
        request_timeout_ms = cfg.getInt(cfg_prefix + ".request_timeout_ms", 30000);
        max_connections = cfg.getInt(cfg_prefix + ".max_connections", 1024);
        slow_read_ms = cfg.getInt(cfg_prefix + ".slow_read_ms", 100);

        region = cfg.getString(cfg_prefix + ".region", "us_east");
        endpoint = cfg.getString(cfg_prefix + ".endpoint", "");
        if (endpoint.empty())
            throw Exception("Endpoint can't be empty, config prefix " + cfg_prefix, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        bucket = cfg.getString(cfg_prefix + ".bucket", "");
        if (bucket.empty())
            throw Exception("Bucket can't be empty, config prefix " + cfg_prefix, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        root_prefix = cfg.getString(cfg_prefix + ".path", "");
        // if (root_prefix.empty() || root_prefix[0] == '/')
        //     throw Exception("Root prefix can't be empty or start with '/'", ErrorCodes::BAD_ARGUMENTS);

        // Not required, we can still obtain this from environment variable
        ak_id = cfg.getString(cfg_prefix + ".ak_id", "");
        ak_secret = cfg.getString(cfg_prefix + ".ak_secret", "");
        session_token = cfg.getString(cfg_prefix + ".session_token", "");
        credential_config = S3::CredentialsConfiguration{
            cfg.getBool(cfg_prefix + ".use_environment_credentials", cfg.getBool("s3.use_environment_credentials", true)),
            cfg.getBool(cfg_prefix + ".use_insecure_imds_request", cfg.getBool("s3.use_insecure_imds_request", false)),
            cfg.getUInt64(cfg_prefix + ".expiration_window_seconds", cfg.getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            cfg.getBool(cfg_prefix + ".no_sign_request", cfg.getBool("s3.no_sign_request", false))
        };

        is_virtual_hosted_style = cfg.getBool(cfg_prefix + ".is_virtual_hosted_style", true);
        http_keep_alive_timeout_ms = cfg.getUInt(cfg_prefix + ".http_keep_alive_timeout_ms", 5000);
        http_connection_pool_size = cfg.getUInt(cfg_prefix + ".http_connection_pool_size", 1024);
        min_upload_part_size = cfg.getUInt64(cfg_prefix + ".min_upload_part_size", 16 * 1024 * 1024);
        max_single_part_upload_size = cfg.getUInt64(cfg_prefix + ".max_single_part_upload_size", 16 * 1024 * 1024);

        if (ak_id.empty())
            collectCredentialsFromEnv();
    }

    void S3Config::collectCredentialsFromEnv()
    {
        static const char * S3_AK_ID = "AWS_ACCESS_KEY_ID";
        static const char * S3_AK_SECRET = "AWS_SECRET_ACCESS_KEY";

        char * env_ak_id = std::getenv(S3_AK_ID);
        if (env_ak_id != nullptr && std::strlen(env_ak_id) != 0)
        {
            ak_id = String(env_ak_id);
        }
        char * env_ak_secret = std::getenv(S3_AK_SECRET);
        if (env_ak_secret != nullptr && std::strlen(env_ak_secret) != 0)
        {
            ak_secret = String(env_ak_secret);
        }
    }

    std::shared_ptr<Aws::S3::S3Client> S3Config::create() const
    {
        std::shared_ptr<Aws::Client::ClientConfiguration> client_cfg = S3::ClientFactory::instance().createClientConfiguration(
            region, RemoteHostFilter(), max_redirects, http_keep_alive_timeout_ms,
            http_connection_pool_size, false);
        client_cfg->endpointOverride = endpoint;
        client_cfg->region = region;
        client_cfg->connectTimeoutMs = connect_timeout_ms;
        client_cfg->requestTimeoutMs = request_timeout_ms;
        client_cfg->maxConnections = max_connections;
        client_cfg->enableTcpKeepAlive = true;

        // update scheme and region based on endpoint
        if (!client_cfg->endpointOverride.empty())
        {
            static const RE2 region_pattern(R"(^s3[.\-]([a-z0-9\-]+)\.amazonaws\.)");
            Poco::URI uri(client_cfg->endpointOverride);
            if (uri.getScheme() == "http")
                client_cfg->scheme = Aws::Http::Scheme::HTTP;

            if (client_cfg->region.empty())
            {
                String matched_region;
                if (re2::RE2::PartialMatch(uri.getHost(), region_pattern, &matched_region))
                {
                    boost::algorithm::to_lower(matched_region);
                    client_cfg->region = matched_region;
                }
                else
                    /// In global mode AWS C++ SDK send `us-east-1` but accept switching to another one if being suggested.
                    client_cfg->region = Aws::Region::AWS_GLOBAL;
            }
        }

        return S3::ClientFactory::instance().create(client_cfg, is_virtual_hosted_style,
                    ak_id, ak_secret, "", {}, credential_config, session_token);
    }


    size_t S3Util::getObjectSize(const String & key) const { return headObject(key).GetContentLength(); }

    std::map<String, String> S3Util::getObjectMeta(const String & key) const { return headObject(key).GetMetadata(); }

    bool S3Util::exists(const String & key) const
    {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);

        ProfileEvents::increment(ProfileEvents::S3HeadObject);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3HeadObject);
        Aws::S3::Model::HeadObjectOutcome outcome = client->HeadObject(request);

        if (outcome.IsSuccess())
        {
            return true;
        }
        else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        {
            return false;
        }
        else
        {
            throw S3Exception(outcome.GetError());
        }
    }

    // -----------------------------------------------------------------------
    // S3 file stream implementations

    // A non-copying iostream.
    // See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
    // https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
    class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
    public:
    StringViewStream(const void* data, int64_t nbytes)
        : Aws::Utils::Stream::PreallocatedStreamBuf(
                reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
                static_cast<size_t>(nbytes)),
            std::iostream(this) {}
    };

    // By default, the AWS SDK reads object data into an auto-growing StringStream.
    // To avoid copies, read directly into our preallocated buffer instead.
    // See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
    // functionally similar recipe.
    Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes) {
    return [=]() { return Aws::New<StringViewStream>("", data, nbytes); };
    }

    bool S3Util::read(const String & key, size_t offset, size_t size, BufferBase::Buffer & buffer) const
    {
        if (size == 0)
        {
            buffer.resize(0);
            return true;
        }

        String range = "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + size - 1);

        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetRange(range);
        req.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer.begin(), size));

        ProfileEvents::increment(ProfileEvents::S3GetObject);
        Aws::S3::Model::GetObjectOutcome outcome = client->GetObject(req);

        if (outcome.IsSuccess())
        {
            // Set throw so we can get fail reason?
            Aws::IOStream & stream = outcome.GetResult().GetBody();
            if (AWSOptionsConfig::instance().use_crt_http_client) {
                if (stream.peek() == EOF)
                    return false;
                stream.seekg(size);
                if (stream.peek() != EOF)
                    throw Exception("Unexpected state of istream", ErrorCodes::S3_ERROR);
            } else {
                stream.read(buffer.begin(), size);
                size_t last_read_count = stream.gcount();
                if (!last_read_count)
                {
                    if (stream.eof())
                        return false;

                    if (stream.fail())
                        throw Exception("Cannot read from istream", ErrorCodes::S3_ERROR);

                    throw Exception("Unexpected state of istream", ErrorCodes::S3_ERROR);
                }
            }

            buffer.resize(size);
            return true;
        }
        else
        {
            // When we reach end of object
            if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE)
            {
                return false;
            }
            throw S3Exception(outcome.GetError());
        }
    }

    S3Util::S3ListResult S3Util::listObjectsWithPrefix(const String & prefix, const std::optional<String> & token, int limit) const
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(bucket);
        request.SetMaxKeys(limit);
        request.SetPrefix(prefix);
        if (token.has_value())
        {
            request.SetContinuationToken(token.value());
        }

        Aws::S3::Model::ListObjectsV2Outcome outcome = client->ListObjectsV2(request);

        if (outcome.IsSuccess())
        {
            S3Util::S3ListResult result;
            const Aws::Vector<Aws::S3::Model::Object> & contents = outcome.GetResult().GetContents();
            result.has_more = outcome.GetResult().GetIsTruncated();
            result.token = outcome.GetResult().GetNextContinuationToken();
            result.object_names.reserve(contents.size());
            result.object_sizes.reserve(contents.size());
            for (const auto & content : contents)
            {
                result.object_names.push_back(content.GetKey());
                result.object_sizes.push_back(content.GetSize());
            }
            return result;
        }
        else
        {
            throw S3Exception(outcome.GetError(), fmt::format("Could not list objects in bucket {} with prefix {}", bucket, prefix));
        }
    }

    String S3Util::createMultipartUpload(
        const String & key,
        const std::optional<std::map<String, String>> & meta,
        const std::optional<std::map<String, String>> & tags) const
    {
        ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);
        Aws::S3::Model::CreateMultipartUploadRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        if (meta.has_value())
        {
            req.SetMetadata(meta.value());
        }
        if (tags.has_value())
        {
            req.SetTagging(urlEncodeMap(tags.value()));
        }

        auto outcome = client->CreateMultipartUpload(req);
        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors, 1);
            throw S3Exception(outcome.GetError());
        }
        return outcome.GetResult().GetUploadId();
    }

    void S3Util::completeMultipartUpload(
        const String & key,
        const String & upload_id,
        const std::vector<String> & etags) const
    {
        if (etags.empty())
        {
            throw Exception("Trying to complete a multiupload without any part in it", ErrorCodes::LOGICAL_ERROR);
        }

        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetUploadId(upload_id);

        ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);
        Aws::S3::Model::CompletedMultipartUpload multipart_upload;
        for (size_t i = 0; i < etags.size(); ++i)
        {
            Aws::S3::Model::CompletedPart part;
            multipart_upload.AddParts(part.WithETag(etags[i]).WithPartNumber(i + 1));
        }

        req.SetMultipartUpload(multipart_upload);

        auto outcome = client->CompleteMultipartUpload(req);

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors, 1);
            throw S3Exception(outcome.GetError());
        }
    }

    void S3Util::abortMultipartUpload(const String & key, const String & upload_id) const
    {
        Aws::S3::Model::AbortMultipartUploadRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetUploadId(upload_id);

        ProfileEvents::increment(ProfileEvents::S3AbortMultipartUpload);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3AbortMultipartUpload);
        auto outcome = client->AbortMultipartUpload(req);

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors, 1);
            throw S3Exception(outcome.GetError());
        }
    }

    String S3Util::uploadPart(
        const String & key,
        const String & upload_id,
        size_t part_number,
        size_t size,
        const std::shared_ptr<Aws::StringStream> & stream) const
    {
        ProfileEvents::increment(ProfileEvents::S3UploadPart);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3UploadPart);
        Aws::S3::Model::UploadPartRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetPartNumber(part_number);
        req.SetUploadId(upload_id);
        req.SetContentLength(size);
        req.SetBody(stream);

        auto outcome = client->UploadPart(req);

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors, 1);
            throw S3Exception(outcome.GetError());
        }
        return outcome.GetResult().GetETag();
    }

    void S3Util::upload(
        const String & key,
        size_t size,
        const std::shared_ptr<Aws::StringStream> & stream,
        const std::optional<std::map<String, String>> & metadata,
        const std::optional<std::map<String, String>> & tags) const
    {
        Aws::S3::Model::PutObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetContentLength(size);
        req.SetBody(stream);
        if (metadata.has_value())
        {
            req.SetMetadata(metadata.value());
        }
        if (tags.has_value())
        {
            req.SetTagging(urlEncodeMap(tags.value()));
        }

        ProfileEvents::increment(ProfileEvents::S3PutObject);
        if (for_disk_s3)
            ProfileEvents::increment(ProfileEvents::DiskS3PutObject);
        auto outcome = client->PutObject(req);

        if (!outcome.IsSuccess())
        {
            ProfileEvents::increment(ProfileEvents::S3WriteRequestsErrors, 1);
            throw S3Exception(outcome.GetError());
        }
    }

    void S3Util::deleteObject(const String & key, bool check_existence) const
    {
        if (check_existence)
        {
            getObjectSize(key);
        }

        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);

        Aws::S3::Model::DeleteObjectOutcome outcome = client->DeleteObject(request);

        if (!outcome.IsSuccess())
        {
            throw S3Exception(outcome.GetError());
        }
    }

    void S3Util::deleteObjects(const std::vector<String> & keys) const
    {
        if (keys.empty())
        {
            return;
        }
        if (keys.size() > 1000)
        {
            throw Exception("The number of s3 deleted objects cannot exceed 1000.", ErrorCodes::BAD_ARGUMENTS);
        }

        std::vector<Aws::S3::Model::ObjectIdentifier> obj_ids;
        obj_ids.reserve(keys.size());
        for (const String & key : keys)
        {
            Aws::S3::Model::ObjectIdentifier obj_id;
            obj_id.SetKey(key);
            obj_ids.push_back(obj_id);
        }
        Aws::S3::Model::Delete delete_objs;
        delete_objs.SetObjects(obj_ids);
        delete_objs.SetQuiet(true);

        ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
        Aws::S3::Model::DeleteObjectsRequest request;
        request.SetBucket(bucket);
        request.SetDelete(delete_objs);

        Aws::S3::Model::DeleteObjectsOutcome outcome = client->DeleteObjects(request);

        if (!outcome.IsSuccess())
        {
            throw S3Exception(outcome.GetError());
        }
        else
        {
            auto str_err = [](const std::vector<Aws::S3::Model::Error> & errs) {
                std::stringstream ss;
                for (size_t i = 0; i < errs.size(); i++)
                {
                    const auto & err = errs[i];
                    ss << "{" << err.GetKey() << ": " << err.GetMessage() << "}";
                }
                return ss.str();
            };
            const std::vector<Aws::S3::Model::Error> & errs = outcome.GetResult().GetErrors();
            if (!errs.empty())
            {
                throw S3Exception(outcome.GetError(), str_err(errs));
            }
        }
    }

    void S3Util::deleteObjectsInBatch(const std::vector<String> & keys, size_t batch_size) const
    {
        for (size_t idx = 0; idx < keys.size(); idx += batch_size)
        {
            size_t end_idx = std::min(idx + batch_size, keys.size());
            deleteObjects(std::vector<String>(keys.begin() + idx, keys.begin() + end_idx));
        }
    }

    void S3Util::deleteObjectsWithPrefix(
        const String & prefix, const std::function<bool(const S3Util &, const String &)> & filter, size_t batch_size) const
    {
        S3Util::S3ListResult result;
        std::vector<String> objects_to_clean;

        do
        {
            result.object_names.clear();

            result = listObjectsWithPrefix(prefix, result.token, batch_size);

            for (const String & name : result.object_names)
            {
                if (filter(*this, name))
                {
                    objects_to_clean.push_back(name);

                    if (objects_to_clean.size() >= batch_size)
                    {
                        deleteObjects(objects_to_clean);
                        objects_to_clean.clear();
                    }
                }
            }
        } while (result.has_more);

        deleteObjects(objects_to_clean);
    }

    String S3Util::urlEncodeMap(const std::map<String, String> & mp)
    {
        Poco::URI uri;
        for (const auto & entry : mp)
        {
            uri.addQueryParameter(fmt::format("{}:{}", entry.first, entry.second));
        }
        return uri.getQuery();
    }

    // Since head object has no http response body, but it won't possible to
    // get actual error message if something goes wrong
    Aws::S3::Model::HeadObjectResult S3Util::headObject(const String & key) const
    {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(key);

        ProfileEvents::increment(ProfileEvents::S3HeadObject);
        Aws::S3::Model::HeadObjectOutcome outcome = client->HeadObject(request);
        if (outcome.IsSuccess())
        {
            return outcome.GetResultWithOwnership();
        }
        else
        {
            throw S3Exception(outcome.GetError());
        }
    }

    Aws::S3::Model::GetObjectResult S3Util::headObjectByGet(const String & key) const
    {
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);
        req.SetRange("bytes=0-1");

        ProfileEvents::increment(ProfileEvents::S3GetObject);
        Aws::S3::Model::GetObjectOutcome outcome = client->GetObject(req);

        if (outcome.IsSuccess())
        {
            return outcome.GetResultWithOwnership();
        }
        else
        {
            throw S3Exception(outcome.GetError());
        }
    }

    S3LazyCleaner::S3LazyCleaner(
        const S3::S3Util & s3_util_,
        const std::function<bool(const S3::S3Util &, const String &)> & filter_,
        size_t max_threads_,
        size_t batch_clean_size_)
        : logger(&Poco::Logger::get("S3LazyCleaner"))
        , batch_clean_size(batch_clean_size_)
        , filter(filter_)
        , s3_util(s3_util_)
        , clean_pool(max_threads_ > 1 ? std::make_unique<ThreadPool>(max_threads_) : nullptr)
    {
    }

    S3LazyCleaner::~S3LazyCleaner() noexcept
    {
        if (clean_pool != nullptr)
            clean_pool->wait();
    }

    void S3LazyCleaner::push(const String & key_)
    {
        auto task = createExceptionHandledJob(
            [this, key_]() {
                if (filter(s3_util, key_))
                {
                    lazyRemove(key_);
                }
                else
                {
                    LOG_TRACE(logger, fmt::format("Skip clean object {} since it's filter out by filter", key_));
                }
            },
            except_hdl);

        if (clean_pool == nullptr)
        {
            task();
        }
        else
        {
            clean_pool->scheduleOrThrowOnError(std::move(task));
        }
    }

    void S3LazyCleaner::finalize()
    {
        if (clean_pool == nullptr)
        {
            lazyRemove(std::nullopt);
        }
        else
        {
            /// In case of out-of-order schedule.
            clean_pool->wait();
            clean_pool->scheduleOrThrowOnError(createExceptionHandledJob([this]() { lazyRemove(std::nullopt); }, except_hdl));
            clean_pool->wait();
        }

        except_hdl.throwIfException();
    }

    void S3LazyCleaner::lazyRemove(const std::optional<String> & key_)
    {
        LOG_TRACE(logger, fmt::format("Lazy remove {}", key_.value_or("NULL")));

        std::vector<String> keys_to_remove_this_round;

        {
            std::lock_guard<std::mutex> lock(remove_keys_mu);

            if (key_.has_value())
            {
                keys_to_remove.push_back(key_.value());
            }

            if (!key_.has_value() || (keys_to_remove.size() >= batch_clean_size))
            {
                keys_to_remove.swap(keys_to_remove_this_round);
            }
        }

        if (!keys_to_remove_this_round.empty())
        {
            s3_util.deleteObjectsInBatch(keys_to_remove_this_round);
        }
    }

    AuthSettings AuthSettings::loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & config)
    {
        auto access_key_id = config.getString(config_elem + ".access_key_id", "");
        auto secret_access_key = config.getString(config_elem + ".secret_access_key", "");
        auto region = config.getString(config_elem + ".region", "");
        auto server_side_encryption_customer_key_base64 = config.getString(config_elem + ".server_side_encryption_customer_key_base64", "");

        std::optional<bool> use_environment_credentials;
        if (config.has(config_elem + ".use_environment_credentials"))
            use_environment_credentials = config.getBool(config_elem + ".use_environment_credentials");

        std::optional<bool> use_insecure_imds_request;
        if (config.has(config_elem + ".use_insecure_imds_request"))
            use_insecure_imds_request = config.getBool(config_elem + ".use_insecure_imds_request");

        std::optional<uint64_t> expiration_window_seconds;
        if (config.has(config_elem + ".expiration_window_seconds"))
            expiration_window_seconds = config.getUInt64(config_elem + ".expiration_window_seconds");

        std::optional<bool> no_sign_request;
        if (config.has(config_elem + ".no_sign_request"))
            no_sign_request = config.getBool(config_elem + ".no_sign_request");

        HTTPHeaderEntries headers;
        Poco::Util::AbstractConfiguration::Keys subconfig_keys;
        config.keys(config_elem, subconfig_keys);
        for (const std::string & subkey : subconfig_keys)
        {
            if (subkey.starts_with("header"))
            {
                auto header_str = config.getString(config_elem + "." + subkey);
                auto delimiter = header_str.find(':');
                if (delimiter == std::string::npos)
                    throw Exception("Malformed s3 header value", ErrorCodes::BAD_ARGUMENTS);
                headers.emplace_back(HTTPHeaderEntry{header_str.substr(0, delimiter), header_str.substr(delimiter + 1, String::npos)});
            }
        }

        return AuthSettings
            {
                std::move(access_key_id), std::move(secret_access_key),
                std::move(region),
                std::move(server_side_encryption_customer_key_base64),
                std::move(headers),
                use_environment_credentials,
                use_insecure_imds_request,
                expiration_window_seconds,
                no_sign_request
            };
    }


    void AuthSettings::updateFrom(const AuthSettings & from)
    {
        /// Update with check for emptyness only parameters which
        /// can be passed not only from config, but via ast.

        if (!from.access_key_id.empty())
            access_key_id = from.access_key_id;
        if (!from.access_key_secret.empty())
            access_key_secret = from.access_key_secret;

        headers = from.headers;
        region = from.region;
        server_side_encryption_customer_key_base64 = from.server_side_encryption_customer_key_base64;
        use_environment_credentials = from.use_environment_credentials;
        use_insecure_imds_request = from.use_insecure_imds_request;
    }

    bool processReadException(Exception & e, Poco::Logger * log, const String & bucket, const String & key, size_t offset, size_t attempt)
    {
        ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors);

        if (log)
            LOG_DEBUG(
                log,
                "Caught exception while reading S3 object. Bucket: {}, Key: {}, Offset: {}, "
                "Attempt: {}, Message: {}",
                bucket, key, offset, attempt, e.message());

        if (auto * s3_exception = dynamic_cast<S3Exception *>(&e))
        {
            /// It doesn't make sense to retry Access Denied or No Such Key
            if (!s3_exception->isRetryableError())
            {
                s3_exception->addMessage("while reading key: {}, from bucket: {}", key, bucket);
                return false;
            }
        }

        /// It doesn't make sense to retry allocator errors
        if (e.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
        {
            if (log)
                tryLogCurrentException(log);
            return false;
        }

        return true;
    }

    void resetSessionIfNeeded(bool read_all_range_successfully, std::optional<Aws::S3::Model::GetObjectResult> & read_result)
    {
        if (!read_result)
            return;

        if (auto session = getSession(*read_result); !session.isNull())
        {
            /// Only connection with fully consumed response can be reused,
            /// otherwise we'll encounter Malformed Message error on next request.
            if (read_all_range_successfully)
            {
                ProfileEvents::increment(ProfileEvents::S3PreservedSessions);
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::S3ResetSessions);
                auto & http_session = static_cast<Poco::Net::HTTPClientSession &>(*session);
                http_session.reset();
            }
        }
    }

}

}

#endif
