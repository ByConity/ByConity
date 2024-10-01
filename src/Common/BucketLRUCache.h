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

#include <list>
#include <memory>
#include <mutex>
#include <concepts>
#include <optional>
#include <unordered_map>
#include <shared_mutex>
#include <Poco/Logger.h>
#include <Core/Types.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>

class BucketLRUCacheTest;

namespace DB
{

template <typename T>
concept LRUCacheWeightType = requires (T lhs, T rhs) {
    lhs == rhs;
    lhs < rhs;
    lhs += rhs;
    lhs -= rhs;
    lhs = rhs;
};

template <typename T, typename TValue, typename TWeight>
concept LRUCacheWeighterType = requires (T weighter, TValue value) {
    { weighter(value) } -> std::same_as<TWeight>;
};

template <typename T, typename TKey>
concept LRUCacheKeyHasherType = requires (T hasher, TKey key) {
    { hasher(key) } -> std::same_as<size_t>;
};

template <typename T>
struct BucketLRUCacheTrivialWeighter
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};

template <size_t DIMENSION>
struct DimensionBucketLRUWeight
{
    DimensionBucketLRUWeight(): weight_container{} {}
    DimensionBucketLRUWeight(const DimensionBucketLRUWeight& rhs)
    {
        operator=(rhs);
    }
    DimensionBucketLRUWeight(const std::initializer_list<size_t>& init_list): weight_container{}
    {
        if (init_list.size() != DIMENSION)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Initializer list have {} argument, "
                "but weight require {}", init_list.size(), DIMENSION);
        }
        size_t idx = 0;
        for (const auto& val : init_list)
        {
            weight_container[idx++] = val;
        }
    }

    inline bool operator<(const DimensionBucketLRUWeight& rhs) const
    {
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            if (weight_container[i] > rhs.weight_container[i])
            {
                return false;
            }
        }
        return true;
    }

    inline bool operator==(const DimensionBucketLRUWeight& rhs) const
    {
        return weight_container == rhs.weight_container;
    }

    inline DimensionBucketLRUWeight& operator+=(const DimensionBucketLRUWeight& rhs)
    {
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            weight_container[i] += rhs.weight_container[i];
        }
        return *this;
    }

    inline DimensionBucketLRUWeight& operator-=(const DimensionBucketLRUWeight& rhs)
    {
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            weight_container[i] -= rhs.weight_container[i];
        }
        return *this;
    }

    inline DimensionBucketLRUWeight& operator=(const DimensionBucketLRUWeight& rhs)
    {
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            weight_container[i] = rhs.weight_container[i].load();
        }
        return *this;
    }

    inline std::atomic<size_t>& operator[](size_t idx)
    {
        return weight_container[idx];
    }

    inline const std::atomic<size_t>& operator[](size_t idx) const
    {
        return weight_container[idx];
    }

    inline bool valid() const
    {
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            if (unlikely(weight_container[i] >= INVALID_THRESHOLD))
            {
                return false;
            }
        }
        return true;
    }

    static constexpr size_t INVALID_THRESHOLD = 1ull << 63;
    std::array<std::atomic<size_t>, DIMENSION> weight_container = {};
};

template <size_t DIMENSION, typename T>
struct TrivialDimensionBucketLRUWeighter
{
    DimensionBucketLRUWeight<DIMENSION> operator()(const T&) const
    {
        DimensionBucketLRUWeight<DIMENSION> weight;
        for (size_t i = 0; i < DIMENSION; ++i)
        {
            weight.weight_container[i] = 1;
        }
        return weight;
    }
};

template <typename TKey, typename TValue, LRUCacheWeightType TWeight = size_t,
    LRUCacheWeighterType<TValue, TWeight> TWeighter = BucketLRUCacheTrivialWeighter<TValue>,
    LRUCacheKeyHasherType<TKey> TKeyHasher = std::hash<TKey>>
class BucketLRUCache
{
public:
    using ValuePtr = std::shared_ptr<TValue>;
    using WeightType = TWeight;
    using LRUCacheType = BucketLRUCache<TKey, TValue, TWeight, TWeighter, TKeyHasher>;

    struct Options
    {
        // Update interval for lru list
        UInt32 lru_update_interval = 60;
        // Bucket size of hash map
        UInt32 mapping_bucket_size = 64;
        // LRU max weight
        TWeight max_weight;
        // Cache entry's minimal ttl, disabled by default
        UInt32 cache_ttl = 0;

        /// Customize evict handler, return a pair, first element indicate
        /// if this element should remove from cache, second element indicate
        /// if this element shouldn't be removed, what's the new value of this
        /// element, if nullptr, leave it unchanged, the new value of entry's
        /// weight must be 0
        std::function<std::pair<bool, ValuePtr>(const TKey&, const TValue&, const TWeight& weight)> evict_handler;

        std::function<void(const TKey&)> insert_callback;
        // Will pass every touched entry to this handler, including dropped
        // updated cache entry, outside lru's lock
        std::function<void(const std::vector<std::pair<TKey, ValuePtr>>&,
            const std::vector<std::pair<TKey, ValuePtr>>&)> post_evict_callback;
    };

    explicit BucketLRUCache(const Options& opts_):
        opts(opts_), logger(&Poco::Logger::get("BucketLRUCache")),
        weighter(), current_weight(), hits(0), misses(0),
        container(opts_.mapping_bucket_size)
    {
        if (opts.mapping_bucket_size == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mapping buckets' size can't "
                "be 0");
        }

        if (opts.evict_handler)
        {
            evict_processor = [this](const TKey& key, const Cell& cell) {
                auto [should_remove, new_val] = opts.evict_handler(
                    key, *cell.value, cell.weight
                );

                TWeight weight {};
                if (!should_remove)
                {
                    weight = new_val == nullptr ? cell.weight : weighter(*new_val);
                }

                return std::pair<bool, std::optional<Cell>>(should_remove, new_val == nullptr ? std::nullopt : std::optional<Cell>(
                    Cell(timestamp(), cell.queue_iterator, new_val, weight)
                ));
            };
        }
    }

    ValuePtr get(const TKey& key_)
    {
        /// Fast path
        {
            std::optional<Cell> cell = container.get(key_, false);
            if (!cell.has_value())
            {
                return nullptr;
            }

            size_t now = timestamp();
            if (now - cell.value().timestamp < opts.lru_update_interval)
            {
                return cell.value().value;
            }
        }

        /// Slow path, get object from container again to prevent it got evict
        /// from cache before acquire lock
        {
            std::lock_guard<std::mutex> lock(mutex);

            std::optional<Cell> cell_to_update = container.get(key_, true);
            if (cell_to_update.has_value())
            {
                queue.splice(queue.end(), queue, cell_to_update.value().queue_iterator);
                return cell_to_update.value().value;
            }
            return nullptr;
        }
    }

    ValuePtr getOrSet(const TKey& key_, const std::function<ValuePtr()>& load_)
    {
        /// Fast path for get
        {
            std::optional<Cell> cell = container.get(key_, false);
            if (cell.has_value()
                && timestamp() - cell.value().timestamp < opts.lru_update_interval)
            {
                return cell.value().value;
            }
        }
        /// Slow path, we either need to change lru order or need load missing cache
        /// entry
        InsertTokenHolder token_holder;
        {
            std::lock_guard<std::mutex> lock(mutex);

            std::optional<Cell> cell_to_update = container.get(key_, true);
            if (cell_to_update.has_value())
            {
                queue.splice(queue.end(), queue, cell_to_update.value().queue_iterator);
                return cell_to_update.value().value;
            }

            /// Cache entry not found, load
            auto& token = insertion_tokens[key_];
            if (token == nullptr)
            {
                token = std::make_shared<InsertToken>(*this);
            }
            token_holder.acquire(&key_, token, lock);
        }

        InsertToken* token = token_holder.token.get();

        bool upserted = false;
        std::vector<std::pair<TKey, ValuePtr>> removed_elements;
        std::vector<std::pair<TKey, ValuePtr>> updated_elements;
        {
            std::lock_guard<std::mutex> token_lock(token->mutex);
            token_holder.cleaned_up = token->cleaned_up;

            if (token->value != nullptr)
            {
                return token->value;
            }
            token->value = load_();

            std::lock_guard<std::mutex> cache_lock(mutex);

            /// Insert the new value only if the token is still in present in insertion_tokens.
            /// (The token may be absent because of a concurrent reset() call).
            if (auto token_iter = insertion_tokens.find(key_);
                token_iter != insertion_tokens.end() && token_iter->second.get() == token)
            {
                upserted = updateWithoutLock(key_, token->value, UpdateMode::EMPLACE,
                    cache_lock, &removed_elements, &updated_elements);
            }

            if (!token->cleaned_up)
            {
                token_holder.release(token_lock, cache_lock);
            }
        }
        if (upserted && opts.post_evict_callback)
        {
            opts.post_evict_callback(removed_elements, updated_elements);
        }
        return token->value;
    }

    bool emplace(const TKey& key_, const ValuePtr& value_)
    {
        return updateAndExecCallback(key_, value_, UpdateMode::EMPLACE);
    }

    void update(const TKey& key_, const ValuePtr& value_)
    {
        updateAndExecCallback(key_, value_, UpdateMode::UPDATE);
    }

    void insert(const TKey& key_, const ValuePtr& value_)
    {
        updateAndExecCallback(key_, value_, UpdateMode::INSERT);
    }

    bool upsert(const TKey& key_, const ValuePtr& value_)
    {
        return updateAndExecCallback(key_, value_, UpdateMode::UPSERT);
    }

    bool erase(const TKey& key_)
    {
        std::lock_guard<std::mutex> lock(mutex);

        return eraseWithoutLock(key_);
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(mutex);

        container.clear();
        queue.clear();
        insertion_tokens.clear();

        current_weight = {};
    }

    const TWeight& weight() const
    {
        return current_weight;
    }

protected:
    bool eraseWithoutLock(const TKey& key_)
    {
        std::optional<Cell> cell = container.remove(key_);
        if (cell.has_value())
        {
            queue.erase(cell.value().queue_iterator);
            current_weight -= cell.value().weight;
            return true;
        }
        return false;
    }

    std::mutex mutex;

private:
    using LRUQueue = std::list<TKey>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    friend class ::BucketLRUCacheTest;

    struct Cell
    {
        Cell(): timestamp(0), value(nullptr), weight() {}
        Cell(size_t timestamp_, LRUQueueIterator iter_, const ValuePtr& value_,
            const TWeight& weight_):
                timestamp(timestamp_), queue_iterator(iter_), value(value_),
                weight(weight_) {}

        size_t timestamp;
        LRUQueueIterator queue_iterator;
        ValuePtr value;
        TWeight weight;
    };

    struct Bucket
    {
        Bucket() = default;
        Bucket(const Bucket& rhs): cells(rhs.cells) {}

        std::shared_mutex mutex;
        std::unordered_map<TKey, Cell, TKeyHasher> cells;
    };

    // A thread safe bucket hash map
    class Container
    {
    public:
        explicit Container(size_t bucket_num_): bucket_num(bucket_num_), buckets(bucket_num_) {}

        // We must already acquire BucketLRUCache's mutex by now
        void set(const TKey& key, Cell&& cell)
        {
            Bucket& bucket = getBucket(key);
            std::lock_guard<std::shared_mutex> lock(bucket.mutex);

            bucket.cells[key] = cell;
        }

        std::optional<Cell> get(const TKey& key, bool update_timestamp)
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
        std::optional<Cell> remove(const TKey& key)
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

        void clear()
        {
            for (Bucket & bucket : buckets)
            {
                std::lock_guard<std::shared_mutex> lock(bucket.mutex);
                bucket.cells.clear();
            }
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
            TWeight new_weight;
            Cell previous_value;
        };

        UpdateResult conditionalUpdate(const TKey& key,
            std::function<std::pair<bool, std::optional<Cell>>(const TKey&, const Cell&)>& processor)
        {
            Bucket& bucket = getBucket(key);
            std::lock_guard<std::shared_mutex> lock(bucket.mutex);

            auto iter = bucket.cells.find(key);
            if (iter == bucket.cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("BucketLRUCache"), "BucketLRUCache become inconsistent, There must be a bug on it");
                abort();
            }

            auto [should_remove, new_value] = processor(key, iter->second);
            if (should_remove)
            {
                Cell cell = iter->second;
                bucket.cells.erase(iter);
                return {UpdateResult::Status::REMOVED, TWeight(), std::move(cell)};
            }
            else
            {
                if (new_value.has_value())
                {
                    Cell cell = iter->second;
                    bucket.cells.insert_or_assign(iter, key, new_value.value());
                    return {UpdateResult::Status::UPDATED,
                        new_value.value().weight, std::move(cell)};
                }
                else
                {
                    return {UpdateResult::Status::UNTOUCHED, iter->second.weight,
                        iter->second};
                }
            }
        }

    private:
        inline Bucket& getBucket(const TKey& key_)
        {
            size_t hash_value = hasher(key_);

            return buckets[hash_value % bucket_num];
        }

        TKeyHasher hasher;
        const size_t bucket_num;
        std::vector<Bucket> buckets;
    };

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(LRUCacheType& cache_) : cleaned_up(false),
            value(nullptr), cache(cache_), refcount(0) {}

        std::mutex mutex;
        bool cleaned_up; /// Protected by the token mutex
        ValuePtr value; /// Protected by the token mutex

        LRUCacheType& cache;
        size_t refcount; /// Protected by the cache mutex
    };

    /// This class is responsible for removing used insert tokens from the insertion_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        InsertTokenHolder(): key(nullptr), token(nullptr), cleaned_up(false) {}

        ~InsertTokenHolder()
        {
            if (!token || cleaned_up)
                return;

            std::lock_guard token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                release(token_lock, cache_lock);
        }

        void acquire(const TKey* key_, const std::shared_ptr<InsertToken>& token_,
            [[maybe_unused]]std::lock_guard<std::mutex>& cache_lock_)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void release([[maybe_unused]]std::lock_guard<std::mutex>& token_lock_,
            [[maybe_unused]]std::lock_guard<std::mutex>& cache_lock_)
        {
            token->cache.insertion_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        const TKey* key;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up;
    };

    enum class UpdateMode
    {
        EMPLACE,
        UPDATE,
        INSERT,
        UPSERT
    };

    static size_t timestamp()
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec;
    }

    /// Return if any insert/update happened
    bool updateWithoutLock(const TKey& key_, const ValuePtr& value_, UpdateMode mode_,
        [[maybe_unused]]std::lock_guard<std::mutex>& cache_lock_,
        std::vector<std::pair<TKey, ValuePtr>>* removed_elements_,
        std::vector<std::pair<TKey, ValuePtr>>* updated_elements_)
    {
        std::optional<Cell> cell = container.get(key_, false);
        LRUQueueIterator iter;
        TWeight increase_weight {};
        if (cell.has_value())
        {
            // Object already in cache, adjust lru list
            switch(mode_)
            {
                case UpdateMode::EMPLACE:
                {
                    return false;
                }
                case UpdateMode::INSERT:
                {
                    throw Exception("Trying to insert value but already in lru",
                        ErrorCodes::BAD_ARGUMENTS);
                }
                default:
                    break;
            }

            queue.splice(queue.end(), queue, cell.value().queue_iterator);

            iter = cell.value().queue_iterator;

            current_weight -= cell.value().weight;
            increase_weight = weighter(*value_);
        }
        else
        {
            // New object, insert into lru list
            if (mode_ == UpdateMode::UPDATE)
            {
                throw Exception("Trying to update value {} but no value found",
                    ErrorCodes::BAD_ARGUMENTS);
            }

            // Check if there are enough space to insert new cache entry
            increase_weight = weighter(*value_);
            TWeight total_weight = current_weight;
            total_weight += increase_weight;
            size_t now = timestamp();
            for (auto queue_iter = queue.begin(), end_iter = queue.end();
                queue_iter != end_iter && !(total_weight < opts.max_weight); ++queue_iter)
            {
                std::optional<Cell> result = container.get(*queue_iter, false);
                if (unlikely(!result.has_value()))
                {
                    LOG_ERROR(logger, "Entry found in queue but not in container");
                    abort();
                }
                if (result.value().timestamp + opts.cache_ttl > now)
                {
                    return false;
                }
                total_weight -= result.value().weight;
            }

            if (opts.insert_callback)
            {
                opts.insert_callback(key_);
            }

            iter = queue.insert(queue.end(), key_);
        }

        container.set(key_, Cell(timestamp(), iter, value_, increase_weight));
        current_weight += increase_weight;

        evictIfNecessary(removed_elements_, updated_elements_);

        return true;
    }

    // Must acquire mutex before call this function
    void evictIfNecessary(std::vector<std::pair<TKey, ValuePtr>>* removed_elements_,
        std::vector<std::pair<TKey, ValuePtr>>* updated_elements_)
    {
        LRUQueue moved_elements;

        for (auto iter = queue.begin();
            iter != queue.end()
                && !(current_weight < opts.max_weight || current_weight == opts.max_weight);)
        {
            const TKey& key = *iter;

            if (evict_processor)
            {
                typename Container::UpdateResult result = container.conditionalUpdate(key, evict_processor);

                switch (result.update_status)
                {
                    case Container::UpdateResult::REMOVED: {
                        removed_elements_->emplace_back(key, result.previous_value.value);

                        current_weight -= result.previous_value.weight;

                        iter = queue.erase(iter);
                        break;
                    }
                    case Container::UpdateResult::UPDATED: {
                        updated_elements_->emplace_back(key, result.previous_value.value);

                        current_weight += result.new_weight;
                        current_weight -= result.previous_value.weight;

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

                current_weight -= cell.value().weight;

                iter = queue.erase(iter);
            }
        }

        if (moved_elements.size() != 0)
        {
            queue.splice(queue.end(), moved_elements);
        }
    }

    bool updateAndExecCallback(const TKey& key_, const ValuePtr& value_,
        UpdateMode mode_)
    {
        std::vector<std::pair<TKey, ValuePtr>> removed_elements;
        std::vector<std::pair<TKey, ValuePtr>> updated_elements;
        {
            std::lock_guard<std::mutex> lock(mutex);
            bool upserted = updateWithoutLock(key_, value_, mode_,
                lock, &removed_elements, &updated_elements);
            if (!upserted)
            {
                return false;
            }
        }

        if (opts.post_evict_callback)
        {
            opts.post_evict_callback(removed_elements, updated_elements);
        }
        return true;
    }

    const Options opts;
    Poco::Logger* logger;

    TWeighter weighter;
    // First return value indiecate if the entry should dropped,
    // second return value is the new value of corresponding key, if it's nullopt
    // leave it unchanged
    std::function<std::pair<bool, std::optional<Cell>>(const TKey&, const Cell&)> evict_processor;

    /// Statistics
    TWeight current_weight;
    std::atomic<size_t> hits;
    std::atomic<size_t> misses;

    Container container;
    LRUQueue queue;
    std::unordered_map<TKey, std::shared_ptr<InsertToken>, TKeyHasher> insertion_tokens;
};

}
