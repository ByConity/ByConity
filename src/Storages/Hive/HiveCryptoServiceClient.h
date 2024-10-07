#pragma once
#include <chrono>
#include <mutex>
#include <Poco/Logger.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Common/Logger.h>
#include <common/types.h>

namespace DB
{
class HiveCryptoServiceClient
{
public:
    using Clock = std::chrono::steady_clock;

    static HiveCryptoServiceClient & instance();

    void init(const Poco::Util::AbstractConfiguration & config);

    String unwrapKey(const String & url_prefix, const String & key_identifier, const String & wrapped_key);

private:
    HiveCryptoServiceClient();

    String unwrapKeyImpl(const String & url_prefix, const String & key_identifier, const String & wrapped_key);

    void curlPostJson(const String & url, const String & request_body, String & response_body);
    void pocoPostJson(const String & url, const String & request_body, String & response_body);

    const int64_t max_concurrency;
    const bool use_cache;
    LRUCache<String, String> encryption_key_cache;
    std::atomic<int64_t> num_in_flight_request{0};

    LoggerPtr log = getLogger("HiveCryptoServiceClient");
};
}
