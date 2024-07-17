#pragma once

#include <memory>

#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Storages/DiskCache/NvmCache.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t MARK_CACHE_OVERHEAD = 128;

    size_t operator()(const MarksInCompressedFile & marks) const
    {
        return marks.allocated_bytes() + MARK_CACHE_OVERHEAD;
    }
};


/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
    using Base = LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
    MarkCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes)
        {
        }

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key);

        return key;
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result.first;
    }

    void setNvmCache(std::shared_ptr<NvmCache> nvm_cache_) { nvm_cache = nvm_cache_; }

private:

    void removeExternal(const Key & key, const MappedPtr & value, size_t size) override
    {
        if (nvm_cache && nvm_cache->isEnabled() && size)
        {
            auto hash_key = HybridCache::makeHashKey(&key, sizeof(Key));
            auto token = nvm_cache->createPutToken(hash_key.key());
            nvm_cache->put(hash_key, std::move(value), std::move(token), [](void * obj) {
                auto * ptr = reinterpret_cast<Mapped *>(obj);
                return HybridCache::BufferView{ptr->size() * sizeof(MarkInCompressedFile), reinterpret_cast<const UInt8 *>(ptr->raw_data())};
            }, HybridCache::EngineTag::MarkCache); 
        }
    }

    MappedPtr loadExternal(const Key & key) override
    {
        if (nvm_cache && nvm_cache->isEnabled())
        {
            auto handle = nvm_cache->find<Mapped>(HybridCache::makeHashKey(&key, sizeof(Key)), [&key, this](std::shared_ptr<void> ptr, HybridCache::Buffer buffer) {
                auto cell = std::static_pointer_cast<Mapped>(ptr);
                auto * begin = reinterpret_cast<MarkInCompressedFile *>(buffer.data());
                auto * end = reinterpret_cast<MarkInCompressedFile *>(buffer.data() + buffer.size());
                // TODO(@max.chenxi): memory copy at here, optimize or not ?
                cell->assign(begin, end);
                setInternal(key, cell, true);
            }, HybridCache::EngineTag::MarkCache);
            if (auto ptr = handle.get())
            {
                auto mapped = std::static_pointer_cast<Mapped>(ptr);
                if (mapped->empty())
                    return nullptr;
                return mapped;
            }
        }
        return nullptr;
    }

    std::shared_ptr<NvmCache> nvm_cache{};
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
