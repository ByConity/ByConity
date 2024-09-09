#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>
#include <boost/container_hash/hash_fwd.hpp>
#include <boost/functional/hash.hpp>
#include <roaring.hh>
#include <Common/BucketLRUCache.h>
#include <Common/ShardCache.h>

namespace ProfileEvents
{
    extern const Event GinIndexFilterResultCacheHit;
    extern const Event GinIndexFilterResultCacheMiss;
}

namespace DB
{

struct GinIdxFilterResult
{
    explicit GinIdxFilterResult(bool match_, const roaring::Roaring* filter_):
        match(match_), filter(filter_ == nullptr ? nullptr : std::make_unique<roaring::Roaring>(*filter_)) {}

    bool match;
    std::unique_ptr<roaring::Roaring> filter;
};

struct GinIdxFilterResultWeightFunction
{
    size_t operator()(const GinIdxFilterResult& result)
    {
        return 16 + (result.filter == nullptr ? 0 : result.filter->cardinality() * 4);
    }
};

class GinIdxFilterResultCacheKey
{
public:
    GinIdxFilterResultCacheKey(const String& part_id_, const String& idx_name_,
        const String& query_id_, const String& range_id_):
            part_id(part_id_), idx_name(idx_name_), query_id(query_id_),
            range_id(range_id_) {}

    bool operator==(const GinIdxFilterResultCacheKey& rhs) const
    {
        return part_id == rhs.part_id && idx_name == rhs.idx_name
            && query_id == rhs.query_id && range_id == rhs.range_id;
    }

    /// Unique id for part, could be part path or something
    String part_id;
    String idx_name;
    /// Unique id for query filter, could be query raw string or something
    String query_id;
    /// Use to mark corresponding filter range if any, could be some serialized data
    String range_id;
};

}

namespace std
{
    template<>
    struct hash<DB::GinIdxFilterResultCacheKey>
    {
        size_t operator()(const DB::GinIdxFilterResultCacheKey& key) const
        {
            size_t res = 0;
            boost::hash_combine(res, key.part_id);
            boost::hash_combine(res, key.idx_name);
            boost::hash_combine(res, key.query_id);
            boost::hash_combine(res, key.range_id);
            return res;
        }
    };
}

namespace DB
{

class GinIdxFilterResultCacheContainer: private BucketLRUCache<GinIdxFilterResultCacheKey, GinIdxFilterResult, size_t, GinIdxFilterResultWeightFunction>
{
public:
    using Base = BucketLRUCache<GinIdxFilterResultCacheKey, GinIdxFilterResult, size_t, GinIdxFilterResultWeightFunction>;
    using WeightType = Base::WeightType;

    struct Options
    {
        size_t max_weight;
    };

    explicit GinIdxFilterResultCacheContainer(const Options& opts):
        Base(Base::Options {
            .max_weight = opts.max_weight,
            .evict_handler = [this](const GinIdxFilterResultCacheKey& key, const GinIdxFilterResult&, const size_t&) {
                removeMapping(key);
                return std::pair<bool, std::shared_ptr<GinIdxFilterResult>>(true, nullptr);
            },
            .insert_callback = [this](const GinIdxFilterResultCacheKey& key) {
                addMapping(key);
            }
        })
    {
    }

    std::shared_ptr<GinIdxFilterResult> getOrSet(const GinIdxFilterResultCacheKey& key,
        const std::function<std::shared_ptr<GinIdxFilterResult>()>& load)
    {
        bool loaded = false;
        std::shared_ptr<GinIdxFilterResult> filter_result =
            Base::getOrSet(key, [&loaded, &load]() {
                loaded = true;
                return load();
            });
        ProfileEvents::increment(loaded ? ProfileEvents::GinIndexFilterResultCacheMiss : ProfileEvents::GinIndexFilterResultCacheHit);
        return filter_result;
    }

    void remove(const String& part_id)
    {
        std::lock_guard<std::mutex> lock(Base::mutex);

        if (auto iter = mappings.find(part_id); iter != mappings.end())
        {
            for (const auto& cache_entry : *(iter->second))
            {
                eraseWithoutLock(GinIdxFilterResultCacheKey(part_id, std::get<0>(cache_entry),
                    std::get<1>(cache_entry), std::get<2>(cache_entry)));
            }

            mappings.erase(iter);
        }
    }

private:
    void addMapping(const GinIdxFilterResultCacheKey& key)
    {
        auto& vals = mappings[key.part_id];
        if (vals == nullptr)
        {
            vals = std::make_unique<ResultCacheIDSet>();
        }

        vals->insert(std::make_tuple(key.idx_name, key.query_id, key.range_id));
    }

    void removeMapping(const GinIdxFilterResultCacheKey& key)
    {
        if (auto iter = mappings.find(key.part_id); iter != mappings.end())
        {
            iter->second->erase(std::make_tuple(key.idx_name, key.query_id, key.range_id));

            if (iter->second->empty())
            {
                mappings.erase(iter);
            }
        }
    }

    /// Protected by Base::mutex
    using ResultCacheID = std::tuple<String, String, String>;
    struct ResultCacheIDHash
    {
        size_t operator()(const ResultCacheID& key) const
        {
            return boost::hash_value(key);
        }
    };
    using ResultCacheIDSet = std::unordered_set<ResultCacheID, ResultCacheIDHash>;

    std::mutex mu;
    std::unordered_map<String, std::unique_ptr<ResultCacheIDSet>> mappings;
};

class GinIdxFilterResultCache
{
public:
    GinIdxFilterResultCache(size_t cache_size, size_t cache_shard):
        containers(cache_shard, GinIdxFilterResultCacheContainer::Options {
            .max_weight = std::max(1ul, cache_size / cache_shard)
        })
    {}

    std::shared_ptr<GinIdxFilterResult> getOrSet(const String& part_id,
        const String& idx_name, const String& query_id, const String& range_id,
        const std::function<std::shared_ptr<GinIdxFilterResult>()>& load)
    {
        GinIdxFilterResultCacheKey key(part_id, idx_name, query_id, range_id);
        return containers.shard(key).getOrSet(key, load);
    }

    void remove(const String& part_id)
    {
        containers.shard(GinIdxFilterResultCacheKey(part_id, "", "", "")).remove(part_id);
    }

private:
    struct CacheKeyHasher
    {
        size_t operator()(const GinIdxFilterResultCacheKey& key) const
        {
            return hasher(key.part_id);
        }

        std::hash<String> hasher;
    };

    ShardCache<GinIdxFilterResultCacheKey, CacheKeyHasher, GinIdxFilterResultCacheContainer> containers;
};

}
