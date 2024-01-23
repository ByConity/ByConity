#include <Common/config.h>

#if USE_AWS_S3

#    include <string>
#    include <IO/OSSCommon.h>
#    include <Interpreters/Context.h>
#    include <Poco/Path.h>
#    include <Poco/URI.h>
#    include <Common/Exception.h>


namespace DB
{

static std::map<String, String> oss_endpoint;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int OSS_ENDPOINT_MISSING;
}

// when region, ak and sk are set in sql we use sql settings.
inline bool useOSSSqlSettings(const ContextPtr & context)
{
    return context->getSettingsRef().oss_region.changed && context->getSettingsRef().oss_access_key.changed
        && context->getSettingsRef().oss_secret_key.changed;
}

Poco::URI verifyOSSURI(const std::string & out_path)
{
    Poco::URI oss_uri(out_path);
    if (oss_uri.getPath().empty() || oss_uri.getHost().empty())
    {
        // oss only supports url like this: oss://bucket/{key}
        // Using Poco::URI to parse it, so the bucket name will be parse to URI.host...
        throw Exception("Invalid oss path.", ErrorCodes::LOGICAL_ERROR);
    }
    return oss_uri;
}

OSSConnectionParams::OSSConnectionParams(
    std::string region, std::string access_key, std::string secret_key, std::string security_token, std::string endpoint)
    : region_(region), access_key_(access_key), secret_key_(secret_key), security_token_(security_token), endpoint_(endpoint)
{
}

OSSConnectionParams OSSConnectionParams::parseOSSFromConfig(Poco::Util::AbstractConfiguration & config)
{
    if (config.has("oss_config"))
    {
        std::string region = config.getString("oss_config.region", "");
        std::string access_key = config.getString("oss_config.access_key", "");
        std::string secret_key = config.getString("oss_config.secret_key", "");
        std::string security_token = config.getString("oss_config.security_token", "");
        parseOSSEndpointFromConfig(config);
        return OSSConnectionParams(region, access_key, secret_key, security_token);
    }
    return OSSConnectionParams();
}

OSSConnectionParams OSSConnectionParams::getOSSSettingsFromContext(const ContextPtr & context)
{
    if (useOSSSqlSettings(context))
    {
        return OSSConnectionParams(
            context->getSettingsRef().oss_region.value,
            context->getSettingsRef().oss_access_key.value,
            context->getSettingsRef().oss_secret_key.value,
            context->getSettingsRef().oss_security_token.value,
            context->getSettingsRef().oss_endpoint.changed ? context->getSettingsRef().oss_endpoint.value : "");
    }
    else
    {
        return context->getOSSConnectParams();
    }
}

void OSSConnectionParams::parseOSSEndpointFromConfig(Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::string> regions;
    config.keys("oss_config.regions", regions);
    for (const String & region : regions)
    {
        if (!region.empty())
        {
            std::string endpoint = config.getString("oss_config.regions." + region, "");
            oss_endpoint[region] = endpoint;
        }
    }
}

S3::S3Config OSSConnectionParams::getS3Config(const OSSConnectionParams & oss_connect_params, const String & bucket)
{
    String endpoint = oss_connect_params.endpoint_;
    if (endpoint.empty())
    {
        endpoint = oss_endpoint[oss_connect_params.region_];
        if (endpoint.empty())
        {
            throw Exception(
                "Missing oss endpoint for region: " + oss_connect_params.region_ + ", please check configuration.",
                ErrorCodes::OSS_ENDPOINT_MISSING);
        }
    }

    return S3::S3Config(
        endpoint,
        oss_connect_params.region_,
        bucket,
        oss_connect_params.access_key_,
        oss_connect_params.secret_key_,
        "",
        oss_connect_params.security_token_,
        true);
}

}

#endif
