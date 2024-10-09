#pragma once

#include <Common/Logger.h>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <IO/WriteHelpers.h>
#include <Interpreters/StorageID.h>
#include <Processors/Chunk.h>
#include <Processors/IntermediateResult/OwnerInfo.h>
#include <Storages/Hive/HiveFile/IHiveFile.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <bthread/condition_variable.h>
#include <parallel_hashmap/phmap.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>

namespace Poco { class Logger; }

namespace ProfileEvents
{
    extern const Event IntermediateResultCacheRefuses;
    extern const Event IntermediateResultCacheReadBytes;
    extern const Event IntermediateResultCacheWriteBytes;
}

namespace DB::IntermediateResult
{

struct CacheKey
{
private:
    String digest;
    String full_table_name;
    OwnerInfo owner_info;
    String to_string;

public:
    CacheKey(const String & digest_, const String & full_table_name_, const OwnerInfo & owner_info_) :
        digest(digest_), full_table_name(full_table_name_), owner_info(owner_info_)
    {
        to_string = fmt::format("{}-{}-{}", digest, full_table_name, owner_info.toString());
    }

    CacheKey(const String & digest_, const String & full_table_name_) :
        digest(digest_), full_table_name(full_table_name_)
    {
        to_string = fmt::format("{}-{}", digest, full_table_name);
    }

    CacheKey cloneWithoutOwnerInfo() const
    {
        return {digest, full_table_name};
    }

    String toString() const { return to_string; }

    bool operator==(const CacheKey & other) const
    {
        return toString() == other.toString();
    }

    bool operator<(const CacheKey & other) const {return to_string < other.to_string; }

};

struct CacheValue
{
    bthread::Mutex mutex;
    std::queue<ChunkPtr> chunks;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> rows = 0;

    CacheValue() = default;

    void addChunk(Chunk & chunk)
    {
        auto chunk_bytes = chunk.bytes();
        bytes += chunk_bytes;
        rows += chunk.getNumRows();
        {
            std::lock_guard<bthread::Mutex> lock(mutex);
            auto chunk_ptr = std::make_shared<Chunk>(std::move(chunk));
            chunks.push(std::move(chunk_ptr));
        }
    }

    // will not be concurrent
    Chunk getChunk()
    {
        if (chunks.empty())
            return Chunk();

        auto res = chunks.front();
        chunks.pop();
        auto chunk_bytes = res->bytes();
        bytes -= chunk_bytes;
        rows -= res->getNumRows();

        ProfileEvents::increment(ProfileEvents::IntermediateResultCacheReadBytes, chunk_bytes);
        return res->clone();
    }

    size_t getBytes() const { return bytes; }
    size_t getRows() const { return rows; }

    auto clone() const
    {
        auto res = std::make_shared<CacheValue>();
        res->chunks = chunks;
        res->bytes = bytes.load(std::memory_order_relaxed);
        res->rows = rows.load(std::memory_order_relaxed);
        return res;
    }
};

struct CacheWeightFunction
{
    size_t operator()(const CacheValue & value) const
    {
        return value.bytes;
    }
};

}

namespace std
{
template <>
struct hash<DB::IntermediateResult::CacheKey>
{
    size_t operator()(const DB::IntermediateResult::CacheKey & cache_key) const
    {
        const auto key_string = cache_key.toString();
        return CityHash_v1_0_2::CityHash64(key_string.data(), key_string.length());
    }
};
}

namespace DB::IntermediateResult
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using CacheValuePtr = std::shared_ptr<CacheValue>;

struct CacheHolder
{
    std::unordered_set<CacheKey> write_cache;
    std::unordered_map<CacheKey, CacheValuePtr> read_cache;
    // if true, a simpler pipeline will be generated
    bool all_part_in_cache = false;
    // if ture, the original pipeline will be generated
    bool all_part_in_storage = false;
    // ensure cache is fully written
    bool early_finish = false;
};
using CacheHolderPtr = std::shared_ptr<DB::IntermediateResult::CacheHolder>;

class CacheManager
{
public:
    explicit CacheManager(size_t max_size_in_bytes);

    template <typename Type>
    void validateIntermediateCache(
        CacheHolderPtr cache_holder,
        const ContextPtr & context,
        const String & digest,
        const String & full_table_name,
        bool has_unique_key,
        UInt64 last_modified_timestamp,
        time_t now,
        const String & name,
        const Type & element,
        std::vector<Type> & new_vectors);

    CacheHolderPtr createCacheHolder(
        const ContextPtr & context,
        const String & digest,
        const StorageID & storage_id,
        const RangesInDataParts & parts_with_ranges,
        RangesInDataParts & new_parts_with_ranges);

    CacheHolderPtr createCacheHolder(
        const ContextPtr & context,
        const String & digest,
        const StorageID & storage_id,
        const HiveFiles & hive_files,
        HiveFiles & new_hive_files);

    void setCache(const CacheKey & key, CacheValuePtr value)
    {
        cache->set(key, value);
        ProfileEvents::increment(ProfileEvents::IntermediateResultCacheWriteBytes, value->getBytes());
    }

    CacheValuePtr getCache(const CacheKey & key) const
    {
        return cache->get(key);
    }

    void emplaceCacheForEmptyResult(const CacheKey & key, Chunk & empty_chunk)
    {
        auto exists = [&](Container::value_type &) {};
        auto emplace = [&](const Container::constructor & ctor) {
            auto value_with_state = std::make_shared<ValueWithState>();
            auto value = std::make_shared<CacheValue>();
            value->addChunk(empty_chunk);
            value_with_state->value = std::move(value);
            value_with_state->state = KeyState::Empty;
            ctor(key, value_with_state);
        };
        uncompleted_cache.lazy_emplace_l(key, exists, emplace);
    }

    CacheValuePtr tryGetUncompletedCache(const CacheKey & key)
    {
        CacheValuePtr value;
        [[maybe_unused]] bool found = uncompleted_cache.if_contains(key, [&](Container::value_type & v) {
            if (v.second->state != KeyState::Refused)
                value = v.second->value;
        });
        return value;
    }

    void modifyKeyStateToRefused(const CacheKey & key)
    {
        uncompleted_cache.modify_if(key, [](Container::value_type & v) {
            if (v.second->state == KeyState::Refused)
                return;
            ProfileEvents::increment(ProfileEvents::IntermediateResultCacheRefuses);
            v.second->state = KeyState::Refused;
            v.second->value.reset();
        });
    }

    void eraseUncompletedCache(const CacheKey & key)
    {
        uncompleted_cache.erase_if(key, [&](Container::value_type & v) { return v.second->state == KeyState::Uncompleted; });
    }

    void setComplete(const CacheKey & key);

    size_t count() const { return cache->count(); }

    size_t weight() const { return cache->weight(); }

    void reset()
    {
        cache->reset();
        uncompleted_cache.clear();
    }

private:
    enum class KeyState : uint8_t
    {
        Empty,
        Missed,
        Refused,
        Uncompleted
    };

    struct ValueWithState
    {
        KeyState state = KeyState::Uncompleted;
        CacheValuePtr value;
    };
    using ValueWithStatePtr = std::shared_ptr<ValueWithState>;

    using Cache = LRUCache<CacheKey, CacheValue, std::hash<CacheKey>, CacheWeightFunction>;
    using Container = phmap::parallel_flat_hash_map<
        CacheKey,
        ValueWithStatePtr,
        phmap::priv::hash_default_hash<CacheKey>,
        phmap::priv::hash_default_eq<CacheKey>,
        phmap::priv::Allocator<phmap::priv::Pair<CacheKey, ValueWithStatePtr>>,
        4,
        bthread::Mutex>;

    // In rare cases, it is possible that the key is already in the cache, but is still placed in the uncompleted_cache and marked as UnCompleted.
    // It's not a problem
    std::shared_ptr<Cache> cache;
    Container uncompleted_cache;
    LoggerPtr log;
};

}

using IntermediateResultCachePtr = std::shared_ptr<DB::IntermediateResult::CacheManager>;
