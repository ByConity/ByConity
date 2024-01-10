#include <Common/config.h>
#include "Interpreters/Context_fwd.h"
#include "S3Common.h"
#include "VETosCommon.h"

#include <string>
#include <Interpreters/Context.h>
#include <IO/VETosCommon.h>
#include <Interpreters/Context.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Common/Exception.h>

namespace DB
{
    
static std::map<String, String> tos_s3_endpoint;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int VETOS_CREATE_UPLOAD_ERROR;
}

//when region, ak and sk are set in sql we use sql settings.
inline bool useTosSqlSettings(const ContextPtr & context)
{
    return context->getSettingsRef().tos_region.changed && context->getSettingsRef().tos_access_key.changed
        && context->getSettingsRef().tos_secret_key.changed;
}

Poco::URI verifyTosURI(const std::string & out_path)
{
    Poco::URI tos_uri(out_path);
    if (tos_uri.getPath().empty() || tos_uri.getHost().empty())
    {
        // Tos only supports url like this: vetos://bucket/{key}
        // Using Poco::URI to parse it, so the bucket name will be parse to URI.host...
        throw Exception("Invalid tos path.", ErrorCodes::LOGICAL_ERROR);
    }
    return tos_uri;
}

VETosConnectionParams::VETosConnectionParams(
    std::string region_, std::string access_key_, std::string secret_key_, std::string endpoint_,
    std::string security_token_, int connect_timeout_ms_, int request_timeout_ms_)
    : region(region_), access_key(access_key_), secret_key(secret_key_), endpoint(endpoint_),
    security_token(security_token_), connect_timeout_ms(connect_timeout_ms_),
    request_timeout_ms(request_timeout_ms_)
{
}

VETosConnectionParams VETosConnectionParams::parseVeTosFromConfig(Poco::Util::AbstractConfiguration & config)
{
    if (config.has("ve_tos_config"))
    {
        std::string region = config.getString("ve_tos_config.region", "");
        std::string access_key = config.getString("ve_tos_config.access_key", "");
        std::string secret_key = config.getString("ve_tos_config.secret_key", "");
        std::string security_token = config.getString("ve_tos_config.security_token", "");
        std::string endpoint = config.getString("ve_tos_config.endpoint", "");
        int connection_timeout = config.getInt("ve_tos_config.tos_connection_timeout", 10000);
        int request_timeout = config.getInt("ve_tos_config.tos_request_timeout", 120000);
        parseVetosS3EndpointFromConfig(config);
        return VETosConnectionParams(region, access_key, secret_key, endpoint, security_token,
            connection_timeout, request_timeout);
    }
    return VETosConnectionParams();
}

VETosConnectionParams VETosConnectionParams::getVETosSettingsFromContext(const ContextPtr & context)
{
    if (useTosSqlSettings(context))
    {
        return VETosConnectionParams(
            context->getSettingsRef().tos_region.value,
            context->getSettingsRef().tos_access_key.value,
            context->getSettingsRef().tos_secret_key.value,
            context->getSettingsRef().tos_endpoint.value,
            context->getSettingsRef().tos_security_token.value,
            context->getSettingsRef().tos_connection_timeout,
            context->getSettingsRef().tos_request_timeout);
    }
    else
    {
        return context->getVETosConnectParams();
    }
}

void VETosConnectionParams::parseVetosS3EndpointFromConfig(Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::string> regions;
    config.keys("ve_tos_config.s3-regions", regions);
    for (const String & region : regions)
    {
        if (!region.empty())
        {
            std::string endpoint = config.getString("ve_tos_config.s3-regions." + region, "");
            tos_s3_endpoint[region] = endpoint;
        }
    }
}

S3::S3Config VETosConnectionParams::getS3Config(const VETosConnectionParams & vetos_connect_params, const String & bucket) {
    String endpoint = vetos_connect_params.endpoint;

    if (endpoint.empty())
    {
        // Try to get endpoint from configuration file(region -> endpoint)
        endpoint = tos_s3_endpoint[vetos_connect_params.region];
        if (endpoint.empty())
        {
            throw Exception("Invalid tos region: " + vetos_connect_params.region + ", please check configuration.",
                ErrorCodes::VETOS_CREATE_UPLOAD_ERROR);
        }
    }

    return S3::S3Config(
        endpoint,
        vetos_connect_params.region,
        bucket,
        vetos_connect_params.access_key,
        vetos_connect_params.secret_key,
        "",
        vetos_connect_params.security_token,
        true,
        vetos_connect_params.connect_timeout_ms,
        vetos_connect_params.request_timeout_ms);
}

}
