#include "Storages/RemoteFile/CnchFileCommon.h"

#include <Interpreters/Context.h>
#include "IO/S3Common.h"

namespace DB
{
void StorageS3Configuration::updateS3Client(const ContextPtr & ctx, const CnchFileArguments & arguments)
{
    if (uri.bucket.find_first_of("*?{") != DB::String::npos)
        throw Exception("Expression can not have wildcards inside bucket name", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto s3_settings = ctx->getStorageS3Settings().getSettings(uri.endpoint);
    s3_settings.rw_settings.updateFromSettingsIfEmpty(ctx->getSettingsRef());

    auth_settings = s3_settings.auth_settings;
    rw_settings = s3_settings.rw_settings;
    max_list_nums = rw_settings.max_list_nums;

    if (!ctx->getSettingsRef().s3_access_key_id.value.empty())
        auth_settings.access_key_id = ctx->getSettingsRef().s3_access_key_id;
    else if (!arguments.access_key_id.empty())
        auth_settings.access_key_id = arguments.access_key_id;

    if (!ctx->getSettingsRef().s3_access_key_secret.value.empty())
        auth_settings.access_key_secret = ctx->getSettingsRef().s3_access_key_secret;
    else if (!arguments.access_key_secret.empty())
        auth_settings.access_key_secret = arguments.access_key_secret;

    std::shared_ptr<Aws::Client::ClientConfiguration> client_configuration =
        S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region, ctx->getRemoteHostFilter(), static_cast<unsigned>(ctx->getSettingsRef().s3_max_redirects),
            ctx->getConfigRef().getUInt("s3.http_keep_alive_timeout_ms", 5000),
            ctx->getConfigRef().getUInt("s3.http_connection_pool_size", 1024), false);

    client_configuration->endpointOverride = uri.endpoint.empty() ? s3_settings.endpoint : uri.endpoint;
    client_configuration->maxConnections = static_cast<unsigned>(rw_settings.max_connections);
    client_configuration->region = auth_settings.region.empty() ? uri.region : auth_settings.region;
    client_configuration->connectTimeoutMs = rw_settings.max_timeout_ms;
    client_configuration->requestTimeoutMs = rw_settings.max_timeout_ms;

    auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.access_key_secret);
    auto headers = auth_settings.headers;
    if (!headers_from_ast.empty())
        headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

    S3::ClientSettings client_settings{uri.is_virtual_hosted_style, isS3ExpressEndpoint(uri.endpoint)};

    client = S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        auth_settings.server_side_encryption_customer_key_base64,
        std::move(headers),
        S3::CredentialsConfiguration
        {
            auth_settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", true)),
            auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            auth_settings.expiration_window_seconds.value_or(
                ctx->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            auth_settings.no_sign_request.value_or(ctx->getConfigRef().getBool("s3.no_sign_request", false))
        });

    LOG_DEBUG(
        &Poco::Logger::get("StorageS3Configuration"),
        fmt::format(
            "update s3 client, config: {}, region = {}, endpoint = {}, bucket = {}, key = {}, ak/sk = {} -> {}",
            uri.toString(),
            auth_settings.region,
            uri.endpoint,
            uri.bucket,
            uri.key,
            auth_settings.access_key_id,
            auth_settings.access_key_secret));
}
}
