#include <Common/config.h>
#include "Interpreters/Context_fwd.h"

#if USE_VE_TOS

#include <string>
#include <Interpreters/Context.h>
#include <IO/VETosCommon.h>
#include <Interpreters/Context.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <TosClientV2.h>
#include <Vetos/utils/LogUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

//when region , ak and sk are set in sql we use sql settings.
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


VETosConnectionParams::VETosConnectionParams() : enable_ssl(false)
{
}

VETosConnectionParams::VETosConnectionParams(
    std::string region_, std::string access_key_, std::string secret_key_, std::string security_token_, bool enable_ssl_)
    : region(region_), access_key(access_key_), secret_key(secret_key_), security_token(security_token_), enable_ssl(enable_ssl_)
{
}

std::unique_ptr<VolcengineTos::TosClientV2> VETosConnectionParams::getVETosClient()
{   
    VolcengineTos::ClientConfig config;
    config.maxRetryCount = 3;
    config.enableVerifySSL = enable_ssl;
    if (!security_token.empty())
    {
        auto cred = VolcengineTos::StaticCredentials(access_key, secret_key, security_token);
        auto client_ptr = std::make_unique<VolcengineTos::TosClientV2>(region, cred, config);
        return client_ptr;
    }
    else
    {
        auto client_ptr = std::make_unique<VolcengineTos::TosClientV2>(region, access_key, secret_key, config);
        return client_ptr;
    }
}

VETosConnectionParams VETosConnectionParams::parseVeTosFromConfig(Poco::Util::AbstractConfiguration & config)
{
    if (config.has("ve_tos_config"))
    {
        std::string region = config.getString("ve_tos_config.region", "");
        std::string access_key = config.getString("ve_tos_config.access_key", "");
        std::string secret_key = config.getString("ve_tos_config.secret_key", "");
        std::string security_token = config.getString("ve_tos_config.security_token", "");
        bool enable_ssl = config.getBool("ve_tos_config.enable_ssl", false);
        return VETosConnectionParams(region, access_key, secret_key, security_token, enable_ssl);
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
            context->getSettingsRef().tos_security_token.value);
    }
    else
    {
        return context->getVETosConnectParams();
    }
}

VolcengineTos::LogLevel parseTosLogLevel(const std::string & log_level)
{
    if (log_level == "info")
    {
        return VolcengineTos::LogLevel::LogInfo;
    }
    else if (log_level == "debug")
    {
        return VolcengineTos::LogLevel::LogDebug;
    }
    else
    {
        return VolcengineTos::LogLevel::LogOff;
    }
}

void VETosConnectionParams::initializeTOSClient(Poco::Util::AbstractConfiguration & config)
{   
    if(config.has("ve_tos_config.log"))
    {       
        std::string  log_file_path = config.getString("ve_tos_config.log.path", "ve_tos.log");
        std::string  log_level = config.getString("ve_tos_config.log.level","LogOff");
        VolcengineTos::LogUtils::SetLogger(log_file_path, "tos-cpp-sdk", parseTosLogLevel(log_level));
    }
    VolcengineTos::InitializeClient();
}

}

#endif
