#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <unistd.h>

#include <IO/BufferWithOwnMemory.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Poco/Logger.h>
#include <Common/HashTable/Hash.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <common/logger_useful.h>
#include <common/unit.h>


namespace ProfileEvents
{
extern const Event UncompressedCacheHits;
extern const Event UncompressedCacheMisses;
extern const Event UncompressedCacheWeightLost;
}

namespace DB
{
struct UncompressedCacheCell
{
    HybridCache::Buffer data;
    size_t compressed_size;
    UInt32 additional_bytes;
};

/** Reference to a piece of memory area, which contains an array of elements.
*/
template <typename T>
class DataRef {
public:
    constexpr DataRef(const T * data_, size_t size_, const std::shared_ptr<UncompressedCacheCell> &cell_holder_) 
        : size(size_), data(data_), cell_holder(cell_holder_) {}

    bool tryToConcat(const T * data_, size_t size_, const std::shared_ptr<UncompressedCacheCell> &cell_holder_) 
    {
        if (data_ == data + size && cell_holder == cell_holder_) {
            size += size_;
            return true;
        } else {
            return false;
        }
    }

    const T* getData() const 
    {
        return data;
    }

    const std::shared_ptr<UncompressedCacheCell> &getCellHolder() const 
    {
        return cell_holder;
    }

    size_t getSize() const 
    {
        return size;
    }

    size_t size = 0;  // number of T, not byte

private:
    const T * data = nullptr;
    std::shared_ptr<UncompressedCacheCell> cell_holder;
};


template <typename T>
class ZeroCopyBuffer {
public:
    void add(const T * data_, size_t size_, const std::shared_ptr<UncompressedCacheCell> &cell_holder) 
    {
        if (data_refs.empty() || !data_refs.back().tryToConcat(data_, size_, cell_holder)) {
            data_refs.emplace_back(data_, size_, cell_holder);
        }
        sz += size_;
    }

    void add(DataRef<T> &o) 
    {
        add(o.getData(), o.getSize(), o.getCellHolder());
    }

    int refCnt() const 
    {
        return data_refs.size();
    }

    const DataRef<T> & getRef(int i) const 
    {
        return data_refs[i];
    }
    

    const std::vector<DataRef<T>> & refs() const 
    {
        return data_refs;
    }

    void clear() 
    {
        data_refs.clear();
        sz = 0;
    }

    size_t size() const {return sz;}

private:
    size_t sz = 0;  // number of T, not byte
    std::vector<DataRef<T>> data_refs;
};

struct UncompressedSizeWeightFunction
{
    size_t operator()(const UncompressedCacheCell & x) const { return x.data.size(); }
};

class NvmCacheTest;
/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class NormalUncompressedCache : public LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    explicit NormalUncompressedCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    virtual ~NormalUncompressedCache() override {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));
        if (result.second)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);

        return result.first;
    }

    void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_) { nvm_cache = nvm_cache_; }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
    }

    void removeExternal(const Key & key, const MappedPtr & value, size_t size) override
    {
        if (nvm_cache && nvm_cache->isEnabled() && size)
        {
            auto hash_key = HybridCache::makeHashKey(&key, sizeof(Key));
            auto token = nvm_cache->createPutToken(hash_key.key());
            nvm_cache->put(hash_key, std::move(value), std::move(token), [](void * obj) {
                auto * ptr = reinterpret_cast<Mapped *>(obj);
                memcpy(ptr->data.data() + ptr->data.size(), &ptr->compressed_size, sizeof(ptr->compressed_size));
                memcpy(
                    ptr->data.data() + ptr->data.size() + sizeof(ptr->compressed_size), &ptr->additional_bytes, sizeof(ptr->additional_bytes));
                return HybridCache::BufferView{
                    ptr->data.size() + sizeof(ptr->compressed_size) + sizeof(ptr->additional_bytes),
                    reinterpret_cast<const UInt8 *>(ptr->data.data())};
        }, HybridCache::EngineTag::UncompressedCache);
        }
    }

    MappedPtr loadExternal(const Key & key) override
    {
        if (nvm_cache && nvm_cache->isEnabled())
        {
            auto handle = nvm_cache->find<Mapped>(HybridCache::makeHashKey(&key, sizeof(Key)), [&key, this](std::shared_ptr<void> ptr, HybridCache::Buffer buffer) {
                auto cell = std::static_pointer_cast<Mapped>(ptr);
                cell->data = std::move(buffer);
                cell->additional_bytes = *reinterpret_cast<UInt32 *>(cell->data.data() + cell->data.size() - sizeof(UInt32));
                cell->compressed_size
                    = *reinterpret_cast<size_t *>(cell->data.data() + cell->data.size() - sizeof(UInt32) - sizeof(size_t));
                cell->data.shrink(cell->data.size() - sizeof(UInt32) - sizeof(size_t));
                setInternal(key, cell, true);
            }, HybridCache::EngineTag::UncompressedCache);
            if (auto ptr = handle.get())
            {
                auto mapped = std::static_pointer_cast<Mapped>(ptr);
                if (mapped->data.isNull())
                    return nullptr;
                return mapped;
            }
        }
        return nullptr;
    }

    std::shared_ptr<NvmCache> nvm_cache{};

    friend class NvmCacheTest;
};

template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class LRUCacheWithNvm : public LRUCache<TKey, TMapped, HashFunction, WeightFunction>
{
public:
    using Base = LRUCache<TKey, TMapped, HashFunction, WeightFunction>;
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    using Base::setInternal;

    ~LRUCacheWithNvm() override = default;
    void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_) { nvm_cache = nvm_cache_; }

    void removeExternal(const Key & key, const MappedPtr & value, size_t size) override
    {
        if (nvm_cache && nvm_cache->isEnabled() && size)
        {
            auto hash_key = HybridCache::makeHashKey(&key, sizeof(Key));
            auto token = nvm_cache->createPutToken(hash_key.key());
            if (!token.isValid())
                return;
            nvm_cache->put(hash_key, std::move(value), std::move(token), [](void * obj) {
                auto * ptr = reinterpret_cast<Mapped *>(obj);
                memcpy(ptr->data.data() + ptr->data.size(), &ptr->compressed_size, sizeof(ptr->compressed_size));
                memcpy(
                    ptr->data.data() + ptr->data.size() + sizeof(ptr->compressed_size), &ptr->additional_bytes, sizeof(ptr->additional_bytes));
                return HybridCache::BufferView{
                    ptr->data.size() + sizeof(ptr->compressed_size) + sizeof(ptr->additional_bytes),
                    reinterpret_cast<const UInt8 *>(ptr->data.data())};
        }, HybridCache::EngineTag::UncompressedCache);
        }
    }

    MappedPtr loadExternal(const Key & key) override
    {
        if (nvm_cache && nvm_cache->isEnabled())
        {
            auto handle = nvm_cache->find<Mapped>(HybridCache::makeHashKey(&key, sizeof(Key)), [&key, this](std::shared_ptr<void> ptr, HybridCache::Buffer buffer) {
                auto cell = std::static_pointer_cast<Mapped>(ptr);
                cell->data = std::move(buffer);
                cell->additional_bytes = *reinterpret_cast<UInt32 *>(cell->data.data() + cell->data.size() - sizeof(UInt32));
                cell->compressed_size
                    = *reinterpret_cast<size_t *>(cell->data.data() + cell->data.size() - sizeof(UInt32) - sizeof(size_t));
                cell->data.shrink(cell->data.size() - sizeof(UInt32) - sizeof(size_t));
                setInternal(key, cell, true);
            }, HybridCache::EngineTag::UncompressedCache);
            if (auto ptr = handle.get())
            {
                auto mapped = std::static_pointer_cast<Mapped>(ptr);
                if (mapped->data.isNull())
                    return nullptr;
                return mapped;
            }
        }
        return nullptr;
    }

private:
    std::shared_ptr<NvmCache> nvm_cache{};

    friend class NvmCacheTest;
};


template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = TrivialWeightFunction<TMapped>,
    size_t BITS_FOR_SHARDS = 8>
class ShardedLRUCache
{
    public:
        using Key = TKey;
        using Mapped = TMapped;
        using MappedPtr = std::shared_ptr<Mapped>;
        using Cell = typename LRUCache<TKey, TMapped, HashFunction, WeightFunction>::Cell;

        static constexpr size_t CACHE_NUM_SHARDS = 1ULL << BITS_FOR_SHARDS;
        static constexpr size_t MAX_BUCKET = CACHE_NUM_SHARDS - 1;

        ShardedLRUCache()= default;
        virtual ~ShardedLRUCache() = default; 

        explicit ShardedLRUCache(size_t _capacity)
        {
            capacity = _capacity;
            const size_t per_shard = (capacity + (CACHE_NUM_SHARDS - 1)) / CACHE_NUM_SHARDS;
            for (auto & shard : cache_shards)
                shard.setCapacity(per_shard);
        }

        virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}

        static size_t getShard(size_t hash)
        {
            return (hash >> (32 - BITS_FOR_SHARDS)) & MAX_BUCKET; 
        }

        virtual size_t getHash(const Key & key) { return HashFunction()(key); }

        MappedPtr get(const Key & key)
        {
            const size_t hash = getHash(key);
            return cache_shards[getShard(hash)].get(key, hash);
        }

        void set(const Key & key, const MappedPtr & mapped)
        {
            const size_t hash = getHash(key);
            return cache_shards[getShard(hash)].set(key, mapped);
        }

        template <typename LoadFunc>
        std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
        {
            const size_t hash = getHash(key);
            return cache_shards[getShard(hash)].getOrSet(key, load_func);
        }

        Cell & setInternal(const Key & key, const MappedPtr & mapped, bool from_external = false)
        {
            const size_t hash = getHash(key);
            return cache_shards[getShard(hash)].setInternal(key, mapped, from_external);
        }

        void getStats(size_t & out_hits, size_t & out_misses) const
        {
            for (auto & shard : cache_shards)
            {
                size_t shard_hits;
                size_t shard_miss;
                shard.getStats(shard_hits, shard_miss);
                out_hits += shard_hits;
                out_misses += shard_miss;
            }
        }

        void remove(const Key & key)
        {
            const uint32_t hash = getHash(key);
            return cache_shards[getShard(hash)].remove(key);            
        }

        size_t weight() const
        {
            size_t total_weight = 0;
            for (auto & shard : cache_shards)
                total_weight += shard.weight();
            return total_weight;
        }

        size_t count() const
        {
            size_t total_count = 0;
            for (auto & shard : cache_shards)
                total_count += shard.count();
            return total_count;
        }

        size_t maxSize() const
        {
            return capacity;
        }

        void reset()
        {
            for (auto & shard : cache_shards)
            {
                shard.reset();
            }
        }
    
        void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_) 
        { 
            for (auto & cache : cache_shards)
                cache.setNvmCache(nvm_cache_);
        }

    private:
        mutable bthread::Mutex mutex;
        size_t capacity;
        LRUCacheWithNvm<TKey, TMapped, HashFunction, WeightFunction> cache_shards[CACHE_NUM_SHARDS];
            /// Override this method if you want to track how much weight was lost in removeOverflow method.
        virtual void removeExternal(const Key & /*key*/, const MappedPtr &/*value*/, size_t /*weight*/) {}
        virtual MappedPtr loadExternal(const Key &) { return MappedPtr(); }
};

/**
 * Shard uncompress cache to reduce cache accessment performance.
 */
class ShardUncompressedCache : public ShardedLRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = ShardedLRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    explicit ShardUncompressedCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    virtual ~ShardUncompressedCache() override {}
    size_t getHash(const Key & key) override { return key; }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));
        if (result.second)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);

        return result.first;
    }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
    }
};

/**
 * Uncompressed cache switch between shard and normal mode.
*/
class UncompressedCache
{
    public:

    using Key = UInt128;
    using Mapped = UncompressedCacheCell;
    using MappedPtr = std::shared_ptr<UncompressedCacheCell>;

    explicit UncompressedCache(size_t max_size_in_bytes, bool shard_mode = false) 
    {
        if (shard_mode)
            shard_cache = std::make_unique<ShardUncompressedCache>(max_size_in_bytes);
        else
            normal_cache = std::make_unique<NormalUncompressedCache>(max_size_in_bytes);
    }

    /// Calculate key from path to file and offset.
    static UInt128 caculateKeyHash(const String & path_to_file, size_t offset)
    {
        UInt128 key;
        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.get128(key);
        return key;
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        if (shard_cache)
            return shard_cache->getOrSet(key, load);
        return normal_cache->getOrSet(key, load);
    }

    void reset()
    {
        if (shard_cache)
            shard_cache->reset();
        
        if (normal_cache)
            normal_cache->reset();
    }

    size_t weight() const
    {
        if (shard_cache)
            return shard_cache->weight();
        return normal_cache->weight();
    }

    size_t count() const
    {
        if (shard_cache)
            return shard_cache->count();
        return normal_cache->count();
    }

    void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_)
    {
        if (shard_cache)
            shard_cache->setNvmCache(nvm_cache_);
        if (normal_cache)
            normal_cache->setNvmCache(nvm_cache_);
    }

private:
    std::unique_ptr<ShardUncompressedCache> shard_cache;
    std::unique_ptr<NormalUncompressedCache> normal_cache;
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
