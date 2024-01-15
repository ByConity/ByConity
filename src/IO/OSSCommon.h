#pragma once

#include <Common/config.h>
#include "Interpreters/Context_fwd.h"

#if USE_AWS_S3

#    include <memory>
#    include <string>
#    include <IO/S3Common.h>
#    include <Poco/Path.h>
#    include <Poco/URI.h>

namespace DB
{

// for oss://bucket/{key} return {key}
inline std::string getOSSKeyFromURI(Poco::URI & oss_uri)
{
    return oss_uri.getPath().substr(1, oss_uri.getPath().size() - 1);
}

Poco::URI verifyOSSURI(const std::string & out_path);

class OSSConnectionParams
{
public:
    std::string region_;
    std::string access_key_;
    std::string secret_key_;
    std::string security_token_;
    std::string endpoint_;

    OSSConnectionParams() = default;
    OSSConnectionParams(
        std::string region, std::string access_key, std::string secret_key, std::string security_token = "", std::string endpoint = "");

    static OSSConnectionParams parseOSSFromConfig(Poco::Util::AbstractConfiguration & config);
    //  1. use sql settings for oss first.
    //  2. if not set in sql, use settings in config.
    static OSSConnectionParams getOSSSettingsFromContext(const ContextPtr & context);

    static void parseOSSEndpointFromConfig(Poco::Util::AbstractConfiguration & config);

    static S3::S3Config getS3Config(const OSSConnectionParams & oss_connect_params, const String & bucket);
};

}

#endif
