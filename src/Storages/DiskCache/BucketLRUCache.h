#pragma once

#include <cstddef>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <atomic>
#include <vector>

#include <Core/Types.h>
#include <Common/Exception.h>
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

template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>,
    typename WeightFunction = BucketLRUCacheTrivialWeightFunction<TMapped>>
class BucketLRUCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    BucketLRUCache(size_t max_size_, size_t lru_update_interval_,
        size_t mapping_bucket_size_):
            container(mapping_bucket_size_), max_size(max_size_),
            lru_update_interval(lru_update_interval_),
            logger(&Poco::Logger::get("BucketLRUCache"))
    {
        if (mapping_bucket_size_ == 0)
        {
            throw Exception("Mapping bucket size can't be 0", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    virtual ~BucketLRUCache() {}

    // Retrieve object from cache, if not exist, return nullptr
    MappedPtr get(const Key& key)
    {
        auto res = getImpl(key);
        if (res)
            ++hits;
        else
            ++misses;

        return res;
    }

    // Write object into cache, if object already exist, update existing value
    void set(const Key& key, const MappedPtr& mapped)
    {
        setImpl(key, mapped);
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
        std::lock_guard lock(mutex);
        return current_size;
    }

protected:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell
    {
        Cell(): value(nullptr), size(0), timestamp(0) {}
        Cell(const MappedPtr& value_, size_t size_, size_t timestamp_,
            LRUQueueIterator iter_):
                value(value_), size(size_), timestamp(timestamp_),
                queue_iterator(iter_) {}

        MappedPtr value;
        size_t size;
        size_t timestamp;
        LRUQueueIterator queue_iterator;
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

    private:
        Bucket& getBucket(const Key& key)
        {
            size_t hash_value = hasher(key);

            return buckets[hash_value % buckets.size()];
        }

        HashFunction hasher;
        std::vector<Bucket> buckets;
    };

    // Remove object from cache, if not exist, do nothing
    void remove(const Key& key)
    {
        std::lock_guard<std::mutex> lock(mutex);

        std::optional<Cell> cell = container.remove(key);
        if (cell.has_value())
        {
            queue.erase(cell.value().queue_iterator);
            current_size -= cell.value().size;
            --current_count;
        }
    }

    std::pair<Container, LRUQueue> replicate()
    {
        std::lock_guard<std::mutex> lock(mutex);

        return std::pair<Container, LRUQueue>(container, queue);
    }

    Container container;

    // Must acquire this lock before update lru list or insert/remove element
    // from lru cache
    mutable std::mutex mutex;

    LRUQueue queue;

private:
    friend class ::BucketLRUCacheTest;

    MappedPtr getImpl(const Key& key)
    {
        // Fast path
        {
            std::optional<Cell> cell = container.get(key, false);
            if (!cell.has_value())
            {
                return nullptr;
            }

            size_t now = timestamp();
            if (now - cell.value().timestamp <= lru_update_interval)
            {
                return cell.value().value;
            }
        }

        // Slow path, get object from container again to prevent it got evict
        // from cache before acquire lock
        {
            std::lock_guard<std::mutex> lock(mutex);

            std::optional<Cell> cell_to_update = container.get(key, true);
            if (cell_to_update.has_value())
            {
                queue.splice(queue.end(), queue, cell_to_update.value().queue_iterator);
            }

            return cell_to_update.value().value;
        }
    }

    void setImpl(const Key& key, const MappedPtr& mapped)
    {
        std::lock_guard<std::mutex> lock(mutex);

        std::optional<Cell> cell = container.get(key, false);
        LRUQueueIterator iter;
        if (cell.has_value())
        {
            // Object already in cache, adjust lru list
            queue.splice(queue.end(), queue, cell.value().queue_iterator);

            iter = cell.value().queue_iterator;

            current_size -= cell.value().size;
        }
        else
        {
            // New object, insert into lru list
            iter = queue.insert(queue.end(), key);

            ++current_count;
        }

        size_t weight = weight_function(*mapped);
        container.set(key, Cell(mapped, weight, timestamp(), iter));
        current_size += weight;

        // Evict if necessary
        removeOverflow();
    }

    // Must acquire mutex before call this function
    void removeOverflow()
    {
        size_t queue_size = queue.size();

        while (current_size > max_size && queue_size > 1)
        {
            const Key& key = queue.front();

            std::optional<Cell> cell = container.remove(key);
            if (!cell.has_value())
            {
                LOG_ERROR(logger, "BucketLRUCache become inconsistent, There must be a bug on it");
                abort();
            }

            removeExternal(key, cell.value().value, cell.value().size);

            current_size -= cell.value().size;
            --current_count;
            --queue_size;

            queue.pop_front();
        }

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(logger, "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    virtual void removeExternal(const Key&, const MappedPtr&, size_t) {}

    static size_t timestamp()
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec;
    }

    size_t current_size = 0;
    std::atomic<size_t> current_count {0};
    const size_t max_size;

    std::atomic<size_t> hits {0};
    std::atomic<size_t> misses {0};

    const size_t lru_update_interval;

    WeightFunction weight_function;

    Poco::Logger* logger;
};

}
