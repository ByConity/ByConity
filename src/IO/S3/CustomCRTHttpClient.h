#pragma once

#include <aws/core/http/crt/CRTHttpClient.h>
#include <Common/HTTPHeaderEntries.h>

namespace DB::S3
{
using HTTPHeaderEntries = std::vector<DB::HTTPHeaderEntry>;


struct CustomCRTHttpClientConfiguration : public Aws::Client::ClientConfiguration
{
    HTTPHeaderEntries extra_headers;
    size_t slow_read_ms{100};
};


class CustomCRTHttpClient : public Aws::Http::CRTHttpClient
{
public:
    CustomCRTHttpClient(const CustomCRTHttpClientConfiguration& clientConfig, Aws::Crt::Io::ClientBootstrap& bootstrap):
        Aws::Http::CRTHttpClient(clientConfig, bootstrap), extra_headers_(clientConfig.extra_headers),
        slow_read_ms_(clientConfig.slow_read_ms) {}

    ~CustomCRTHttpClient() override = default;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest> & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

private:
    const HTTPHeaderEntries extra_headers_;
    size_t slow_read_ms_{100};
};

}
