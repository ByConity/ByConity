#include "Storages/RemoteFile/CnchFileCommon.h"

#include <Interpreters/Context.h>

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
        auth_settings.region, ctx->getRemoteHostFilter(), static_cast<unsigned>(ctx->getSettingsRef().s3_max_redirects),
        ctx->getConfigRef().getUInt("s3.http_keep_alive_timeout_ms", 5000),
        ctx->getConfigRef().getUInt("s3.http_connection_pool_size", 1024), false);

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
}
