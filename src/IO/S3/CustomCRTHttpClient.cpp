#include <fmt/core.h>
#include <common/scope_guard.h>
#include "CustomCRTHttpClient.h"
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event CRTHTTPS3GetCount;
    extern const Event CRTHTTPS3GetTime;
    extern const Event CRTHTTPS3WriteBytes;
}

namespace DB::S3
{


std::shared_ptr<Aws::Http::HttpResponse> CustomCRTHttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    for (DB::HTTPHeaderEntry entry : extra_headers_) {
        request->SetHeaderValue(entry.name, entry.value);
    }

    bool is_get_req = request->GetMethod() == Aws::Http::HttpMethod::HTTP_GET;
    Stopwatch total_watch;
    SCOPE_EXIT({
        if (is_get_req) {
            auto time = total_watch.elapsedMicroseconds();
            ProfileEvents::increment(ProfileEvents::CRTHTTPS3GetCount);
            ProfileEvents::increment(ProfileEvents::CRTHTTPS3GetTime, total_watch.elapsedMicroseconds());
            if (slow_read_ms_ > 0 && time >= slow_read_ms_ * 1000) {
                LoggerPtr log = getLogger("AWSClient");
                LOG_DEBUG(log, fmt::format("AWS S3 slow read(over {}ms): {}, time = {}ms",
                    slow_read_ms_, request->GetUri().GetURIString(), time/1000));
            }
        }
    });

    return Aws::Http::CRTHttpClient::MakeRequest(request, readLimiter, writeLimiter);
}

}
