#pragma once

#include <Common/config.h>
#include "Interpreters/Context_fwd.h"

#if USE_VE_TOS

#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <Poco/Path.h>
#include <memory>
#include <string>
#include <TosClientV2.h>

namespace DB
{


/// The min size of the VE_TOS I/O buffer by default 5MB
constexpr auto VE_TOS_DEFAULT_BUFFER_SZIE = 5242880ULL;

// for ve_tos vetos://bucket/{key} return {key} 
inline std::string getTosKeyFromURI(Poco::URI & tos_uri)
{
    return tos_uri.getPath().substr(1, tos_uri.getPath().size() - 1);
}

inline UInt64 getWriteBufferSizeForVETos(const ContextPtr & context)
{
    UInt64 outfile_buffer_size = 1024 * 1024 * context->getSettingsRef().outfile_buffer_size_in_mb;
    return (outfile_buffer_size > VE_TOS_DEFAULT_BUFFER_SZIE) ? outfile_buffer_size : VE_TOS_DEFAULT_BUFFER_SZIE;
}

Poco::URI verifyTosURI(const std::string & out_path);

class VETosConnectionParams
{
public:
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string security_token;
    bool enable_ssl;  

    VETosConnectionParams();
    VETosConnectionParams(
        std::string region, std::string access_key, std::string secret_key, std::string security_token = "", bool enable_ssl = false);

    std::unique_ptr<VolcengineTos::TosClientV2> getVETosClient();

    static VETosConnectionParams parseVeTosFromConfig(Poco::Util::AbstractConfiguration & config);
    //  1. use sql settings for vetos first.
    //  2. if not set in sql, use settings in config.
    static VETosConnectionParams getVETosSettingsFromContext(const ContextPtr& context);

    static void initializeTOSClient(Poco::Util::AbstractConfiguration & config);

};

}

#endif
