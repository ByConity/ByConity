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

    std::shared_ptr<NvmCache> nvm_cache{};

    friend class NvmCacheTest;
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
