#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <utility>
#include <common/types.h>
#include <aws/core/client/ClientConfiguration.h> // Y_IGNORE
#include <aws/core/http/HttpRequest.h>
#include <IO/S3/PocoHTTPClient.h>
#include <Poco/URI.h>

namespace DB::S3
{
class ProxyConfiguration
{
public:
    virtual ~ProxyConfiguration() = default;
    /// Returns proxy configuration on each HTTP request.
    virtual ClientConfigurationPerRequest getConfiguration(const Aws::Http::HttpRequest & request) = 0;
};

}

#endif
