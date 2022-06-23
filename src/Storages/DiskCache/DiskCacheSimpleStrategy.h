#pragma once

#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
class DiskCacheSimpleStrategy : public IDiskCacheStrategy
{
public:
    explicit DiskCacheSimpleStrategy(const DiskCacheStrategySettings & settings_)
        : IDiskCacheStrategy(settings_)
        , cache_statistics(settings_.stats_bucket_size)
        , segment_hits_to_cache(settings_.hits_to_cache)
        , logger(&Poco::Logger::get("DiskCacheSimpleStrategy"))
    {
    }

    virtual IDiskCacheSegmentsVector getCacheSegments(const IDiskCacheSegmentsVector & segments) override;

private:
    struct AccessStatistics
    {
        std::mutex stats_mutex;
        std::unordered_map<String, UInt32> access_stats;
    };

    struct CacheStatistics
    {
        explicit CacheStatistics(size_t bucket_size) : buckets(bucket_size) { }
        AccessStatistics & getAccessStats(const String & key) { return buckets[hasher(key) % buckets.size()]; }

        std::hash<String> hasher;
        std::vector<AccessStatistics> buckets;
    };

    CacheStatistics cache_statistics;

    size_t segment_hits_to_cache;
    Poco::Logger * logger;
};

}
