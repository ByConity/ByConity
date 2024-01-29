/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <atomic>
#include <vector>
#include <fmt/format.h>

#include <Core/Types.h>
#include "common/types.h"
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

class BucketLRUCacheTest;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

template <typename T>
struct BucketLRUCacheTrivialWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};

// NOTE(wsy): All get operation will return pointer of same object, maybe upper level
// need to make it immutable
// NOTE(wsy): Maybe try other data structure like skip list rather than unordered_map
// BucketLRUCache support customize evict handler, user will pass a processor to lru cache
// and processing the cache entry which need to evict, lru will modify cache entry base
// on this evict handler, it could be drop cache entry or update entry without dropping it.
// If cache entry is updated, the weight of cache entry should become 0, and lru cache will
// update it's access time and lru order
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>,
    typename WeightFunction = BucketLRUCacheTrivialWeightFunction<TMapped>>
class BucketLRUCache final
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    struct Options
    {
        // Update interval for lru list
        UInt32 lru_update_interval = 60;
        // Bucket size of hash map
        UInt32 mapping_bucket_size = 64;
        // LRU max weight
        UInt64 max_size = 1;
        UInt64 max_nums = 1;

        bool enable_customize_evict_handler = false;
        // Customize evict handler, return a pair, first element indicate
        // if this element should remove from cache, second element indicate
        // if this element shouldn't be removed, what's the new value of this
        // element, if nullptr, leave it unchanged, the new value of entry's
        // weight must be 0
        std::function<std::pair<bool, MappedPtr>(const Key&, const MappedPtr&, size_t)> customize_evict_handler =
            [](const Key&, const MappedPtr&, size_t) { return std::pair<bool, MappedPtr>(true, nullptr); };

        // Will pass every touched entry to this handler, including dropped
        // updated cache entry, outside lru's lock
        std::function<void(const std::vector<std::pair<Key, MappedPtr>>&, const std::vector<std::pair<Key, MappedPtr>>&)> customize_post_evict_handler =
            [](const std::vector<std::pair<Key, MappedPtr>>&, const std::vector<std::pair<Key, MappedPtr>>&) {};
    };

    explicit BucketLRUCache(const Options& opts_):
        opts(opts_), evict_processor([this](const Key& key, const Cell& cell) {
            auto [should_remove, new_val] = opts.customize_evict_handler(
                key, cell.value, cell.size
            );

            size_t weight = 0;
            if (!should_remove)
            {
                weight = new_val == nullptr ? cell.size : weight_function(*new_val);
            }

            if (unlikely(weight != 0))
            {
                LOG_ERROR(logger, "After evict handler, object still have positive weight");
                abort();
            }

            return std::pair<bool, std::optional<Cell>>(should_remove, new_val == nullptr ? std::nullopt : std::optional<Cell>(
                Cell(weight, timestamp(), cell.queue_iterator, new_val)
            ));
        }), container(opts_.mapping_bucket_size), logger(&Poco::Logger::get("BucketLRUCache"))
    {
        if (opts.mapping_bucket_size <= 0 || opts.max_size <= 0 || opts.max_nums <= 0)
        {
            throw Exception("Mapping bucket size or lru size or lru nums can't be less 0", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    ~BucketLRUCache() = default;

    // Retrieve object from cache, if not exist, return nullptr
    MappedPtr get(const Key& key)
    {
        auto val_pair = fastGet(key);
        if (val_pair.first && !val_pair.second)
        {
            ++hits;
            return val_pair.first;
        }

        if (val_pair.first)
        {
            ++hits;
            std::lock_guard cache_lock(mutex);
            normalGet(key, cache_lock);
            return val_pair.first;
        }

        ++misses;
        return nullptr;
    }

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto val_pair = fastGet(key);
        if (val_pair.first && !val_pair.second)
        {
            ++hits;
            return std::make_pair(val_pair.first, false);
        }

        InsertTokenHolder token_holder;
        {
            std::lock_guard cache_lock(mutex);
            if (val_pair.first)
            {
                ++hits;
                normalGet(key, cache_lock);
                return std::make_pair(val_pair.first, false);
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::lock_guard token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            return std::make_pair(token->value, false);
        }

        ++misses;
        token->value = load_func();

        bool result = false;
        std::vector<std::pair<Key, MappedPtr>> removed_elements;
        std::vector<std::pair<Key, MappedPtr>> updated_elements;
        {
            std::lock_guard cache_lock(mutex);

            /// Insert the new value only if the token is still in present in insert_tokens.
            /// (The token may be absent because of a concurrent reset() call).
            auto token_it = insert_tokens.find(key);
            if (token_it != insert_tokens.end() && token_it->second.get() == token)
            {
                setRetEvictWithLock(key, token->value, SetMode::EMPLACE, 
                    removed_elements, updated_elements, cache_lock);
                result = true;
            }

            if (!token->cleaned_up)
                token_holder.cleanup(token_lock, cache_lock);
        }

        opts.customize_post_evict_handler(removed_elements, updated_elements);

        return std::make_pair(token->value, result);
    }

    bool emplace(const Key& key, const MappedPtr& mapped)
    {
        return setImpl(key, mapped, SetMode::EMPLACE);
    }

    void update(const Key& key, const MappedPtr& mapped)
    {
        setImpl(key, mapped, SetMode::UPDATE);
    }

    bool upsert(const Key& key, const MappedPtr& mapped)
    {
        return setImpl(key, mapped, SetMode::UPSERT);
    }

    void insert(const Key& key, const MappedPtr& mapped)
    {
        setImpl(key, mapped, SetMode::INSERT);
    }

    bool erase(const Key& key)
    {
        std::lock_guard<std::mutex> lock(mutex);

        std::optional<Cell> cell = container.remove(key);
        if (cell.has_value())
        {
            queue.erase(cell.value().queue_iterator);
            current_size -= cell.value().size;
            --current_count;

            return true;
        }
        return false;
    }

    void getStats(size_t& hit_counts, size_t& miss_counts)
    {
        hit_counts = hits;
        miss_counts = misses;
    }

    size_t count() const
    {
        return current_count;
    }

    size_t weight() const
    {
        return current_size;
    }

private:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    friend class ::BucketLRUCacheTest;

    struct Cell
    {
        Cell(): size(0), timestamp(0), value(nullptr) {}
        Cell(size_t size_, size_t timestamp_, LRUQueueIterator iter_,
            const MappedPtr& value_):
                size(size_), timestamp(timestamp_), queue_iterator(iter_),
                value(value_) {}

        size_t size;
        size_t timestamp;
        LRUQueueIterator queue_iterator;
        MappedPtr value;
    };

    struct Bucket
    {
        Bucket() = default;
        Bucket(const Bucket& rhs): cells(rhs.cells) {}

        std::shared_mutex mutex;
        std::unordered_map<Key, Cell, HashFunction> cells;
    };

    // A thread safe bucket hash map
    class Container
    {
    public:
        Container(size_t bucket_num): buckets(bucket_num) {}

        // We must already acquire BucketLRUCache's mutex by now
        void set(const Key& key, Cell&& cell)
        {
            Bucket& bucket = getBucket(key);
            std::lock_guard<std::shared_mutex> lock(bucket.mutex);

            bucket.cells[key] = cell;
        }

        std::optional<Cell> get(const Key& key, bool update_timestamp)
        {
            Bucket& bucket = getBucket(key);

            if (update_timestamp)
            {
                // Need to update cell's last update timestamp, use exclusive lock
                std::lock_guard<std::shared_mutex> lock(bucket.mutex);

                auto iter = bucket.cells.find(key);
                if (iter == bucket.cells.end())
                {
                    return std::nullopt;
                }
                iter->second.timestamp = timestamp();
                return iter->second;
            }
            else
            {
                // Only retrieve cell's data, use shared lock
                std::shared_lock<std::shared_mutex> lock(bucket.mutex);

                auto iter = bucket.cells.find(key);
                if (iter == bucket.cells.end())
                {
                    return std::nullopt;
                }
                return iter->second;
            }
        }

        // Remove object from cells, return origin value, if not exist
        // return nullopt
        std::optional<Cell> remove(const Key& key)
        {
            Bucket& bucket = getBucket(key);
            std::lock_guard<std::shared_mutex> lock(bucket.mutex);

            auto iter = bucket.cells.find(key);
            if (iter == bucket.cells.end())
            {
                return std::nullopt;
            }

            Cell cell = iter->second;
            bucket.cells.erase(iter);
            return cell;
        }

        struct UpdateResult
        {
            enum Status
            {
                REMOVED,
                UPDATED,
                UNTOUCHED,
            };

            Status update_status;
            Cell previous_value;
        };

        UpdateResult conditionalUpdate(const Key& key,
            std::function<std::pair<bool, std::optional<Cell>>(const Key&, const Cell&)>& processor)
        {
            Bucket& bucket = getBucket(key);
            std::lock_guard<std::shared_mutex> lock(bucket.mutex);

            auto iter = bucket.cells.find(key);
            if (iter == bucket.cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("BucketLRUCache"), "ucketLRUCache become inconsistent, There must be a bug on it");
                abort();
            }

            auto [should_remove, new_value] = processor(key, iter->second);
            if (should_remove)
            {
                Cell cell = iter->second;
                bucket.cells.erase(iter);
                return {UpdateResult::Status::REMOVED, std::move(cell)};
            }
            else
            {
                if (new_value.has_value())
                {
                    Cell cell = iter->second;
                    bucket.cells.insert_or_assign(iter, key, new_value.value());
                    return {UpdateResult::Status::UPDATED, std::move(cell)};
                }
                else
                {
                    return {UpdateResult::Status::UNTOUCHED, iter->second};
                }
            }
        }

    private:
        Bucket& getBucket(const Key& key)
        {
            size_t hash_value = hasher(key);

            return buckets[hash_value % buckets.size()];
        }

        HashFunction hasher;
        std::vector<Bucket> buckets;
    };

    // return (value, exceed_update_interval) pair
    std::pair<MappedPtr, bool>  fastGet(const Key& key)
    {
        std::optional<Cell> cell = container.get(key, false);
        if (!cell.has_value())
        {
            return std::make_pair(nullptr, false);
        }

        size_t now = timestamp();
        if (now - cell.value().timestamp < opts.lru_update_interval)
        {
            return std::make_pair(cell.value().value, false);
        }

        return std::make_pair(cell.value().value, true);
    }

    MappedPtr normalGet(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
    {
        std::optional<Cell> cell_to_update = container.get(key, true);
        if (cell_to_update.has_value())
        {
            queue.splice(queue.end(), queue, cell_to_update.value().queue_iterator);
            return cell_to_update.value().value;
        }

        return nullptr;
    }

    enum class SetMode
    {
        EMPLACE,
        UPDATE,
        INSERT,
        UPSERT,
    };

    // Return if any value has been inserted
    bool setImpl(const Key& key, const MappedPtr& mapped, SetMode mode)
    {
        std::vector<std::pair<Key, MappedPtr>> removed_elements;
        std::vector<std::pair<Key, MappedPtr>> updated_elements;

        auto res = false;
        {
            std::lock_guard<std::mutex> guard(mutex);
            res = setRetEvictWithLock(key, mapped, mode, removed_elements, updated_elements, guard);
            if (!res)
                return res;
        }

        opts.customize_post_evict_handler(removed_elements, updated_elements);

        return res;
    }

    bool setRetEvictWithLock(const Key & key, const MappedPtr & mapped, SetMode mode
        , std::vector<std::pair<Key, MappedPtr>> removed_elements
        , std::vector<std::pair<Key, MappedPtr>> updated_elements
        , [[maybe_unused]]std::lock_guard<std::mutex> & guard)
    {
        std::optional<Cell> cell = container.get(key, false);
        LRUQueueIterator iter;
        if (cell.has_value())
        {
            // Object already in cache, adjust lru list
            switch(mode)
            {
                case SetMode::EMPLACE:
                {
                    return false;
                }
                case SetMode::INSERT:
                {
                    throw Exception(fmt::format("Trying to insert value {} but already in lru",
                        toString(key)), ErrorCodes::BAD_ARGUMENTS);
                }
                default:
                    break;
            }

            queue.splice(queue.end(), queue, cell.value().queue_iterator);

            iter = cell.value().queue_iterator;

            current_size -= cell.value().size;
        }
        else
        {
            // New object, insert into lru list
            if (mode == SetMode::UPDATE)
            {
                throw Exception(fmt::format("Trying to update value {} but no value found",
                    toString(key)), ErrorCodes::BAD_ARGUMENTS);
            }

            iter = queue.insert(queue.end(), key);

            ++current_count;
        }

        size_t weight = mapped ? weight_function(*mapped) : 0;
        container.set(key, Cell(weight, timestamp(), iter, mapped));
        current_size += weight;

        evictIfNecessary(removed_elements, updated_elements);

        return true;
    }

    // Must acquire mutex before call this function
    void evictIfNecessary(std::vector<std::pair<Key, MappedPtr>>& removed_elements,
        std::vector<std::pair<Key, MappedPtr>>& updated_elements)
    {
        LRUQueue moved_elements;

        for (auto iter = queue.begin();
            iter != queue.end() && (current_size > opts.max_size || current_count > opts.max_nums);)
        {
            const Key& key = *iter;

            if (opts.enable_customize_evict_handler)
            {
                typename Container::UpdateResult result = container.conditionalUpdate(key, evict_processor);

                current_size -= result.previous_value.size;

                switch (result.update_status)
                {
                    case Container::UpdateResult::REMOVED: {
                        removed_elements.emplace_back(key, result.previous_value.value);

                        --current_count;
                        iter = queue.erase(iter);
                        break;
                    }
                    case Container::UpdateResult::UPDATED: {
                        updated_elements.emplace_back(key, result.previous_value.value);

                        [[fallthrough]];
                    }
                    case Container::UpdateResult::UNTOUCHED: {
                        // If this cache entry is only updated, move it's element into
                        // another temporary list
                        auto moved_iter = iter;
                        ++iter;
                        moved_elements.splice(moved_elements.end(), queue, moved_iter);
                        break;
                    }
                }
            }
            else
            {
                std::optional<Cell> cell = container.remove(key);
                if (unlikely(!cell.has_value()))
                {
                    LOG_ERROR(logger, "BucketLRUCache become inconsistent, There must be a bug on it");
                    abort();
                }

                current_size -= cell.value().size;
                --current_count;

                iter = queue.erase(iter);
            }

            if (unlikely(current_size > (1ull << 63)))
            {
                LOG_ERROR(logger, "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }
        }

        if (moved_elements.size() != 0)
        {
            queue.splice(queue.end(), moved_elements);
        }
    }

    static size_t timestamp()
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec;
    }

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(BucketLRUCache & cache_) : cache(cache_) {}

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        MappedPtr value; /// Protected by the token mutex

        BucketLRUCache & cache;
        size_t refcount = 0; /// Protected by the cache mutex
    };

    using InsertTokenById = std::unordered_map<Key, std::shared_ptr<InsertToken>, HashFunction>;

    /// This class is responsible for removing used insert tokens from the insert_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        const Key * key = nullptr;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up = false;

        InsertTokenHolder() = default;

        void acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup([[maybe_unused]] std::lock_guard<std::mutex> & token_lock, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            token->cache.insert_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        ~InsertTokenHolder()
        {
            if (!token)
                return;

            if (cleaned_up)
                return;

            std::lock_guard token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;

    InsertTokenById insert_tokens;

    const Options opts;

    WeightFunction weight_function;
    // First return value indiecate if the entry should dropped,
    // second return value is the new value of corresponding key, if it's nullopt
    // leave it unchanged
    std::function<std::pair<bool, std::optional<Cell>>(const Key&, const Cell&)> evict_processor;

    // Statistics
    std::atomic<size_t> current_size {0};
    std::atomic<size_t> current_count {0};
    std::atomic<size_t> hits {0};
    std::atomic<size_t> misses {0};

    Container container;

    // Must acquire this lock before update lru list or insert/remove element
    // from lru cache
    mutable std::mutex mutex;

    LRUQueue queue;

    Poco::Logger* logger;
};

}
