#pragma once

#include <Common/config.h>
#include "Interpreters/Context_fwd.h"
#include "S3Common.h"
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <Poco/Path.h>
#include <memory>
#include <string>

namespace DB
{

/// The min size of the VE_TOS I/O buffer by default 5MB
constexpr auto VE_TOS_DEFAULT_BUFFER_SZIE = 5242880ULL;

// for ve_tos vetos://bucket/{key} return {key} 
inline std::string getTosKeyFromURI(Poco::URI & tos_uri)
{
    return tos_uri.getPath().substr(1, tos_uri.getPath().size() - 1);
}

Poco::URI verifyTosURI(const std::string & out_path);

class VETosConnectionParams
{
public:
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string security_token;

    VETosConnectionParams() = default;
    VETosConnectionParams(
        std::string region, std::string access_key, std::string secret_key, std::string security_token = "");

    static VETosConnectionParams parseVeTosFromConfig(Poco::Util::AbstractConfiguration & config);
    //  1. use sql settings for vetos first.
    //  2. if not set in sql, use settings in config.
    static VETosConnectionParams getVETosSettingsFromContext(const ContextPtr& context);

    static void parseVetosS3EndpointFromConfig(Poco::Util::AbstractConfiguration & config);

    static S3::S3Config getS3Config(const VETosConnectionParams & vetos_connect_params, const String & bucket);
};

}
