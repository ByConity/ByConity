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

struct UncompressedSizeWeightFunction
{
    size_t operator()(const UncompressedCacheCell & x) const { return x.data.size(); }
};

class NvmCacheTest;
/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    explicit UncompressedCache(size_t max_size_in_bytes) : Base(max_size_in_bytes)
    {
    }

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset)
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

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
