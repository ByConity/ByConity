#pragma once

#include <memory>
#include <new>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/fibers/TimedMutex.h>

#include <Storages/DiskCache/AbstractCache.h>
#include <Storages/DiskCache/BigHash.h>
#include <Storages/DiskCache/BlockCache.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Contexts.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/Handle.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/InFlightPuts.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/Types.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>
#include <common/unit.h>

namespace DB
{
using EncodeCallback = std::function<HybridCache::BufferView(void *)>;
using DecodeCallback = std::function<void(std::shared_ptr<void>, HybridCache::Buffer)>;
using folly::fibers::TimedMutex;

class NvmCache final : public HybridCache::AbstractCache
{
public:
    using EnginePairSelector = std::function<size_t(HybridCache::EngineTag)>;

    struct Pair
    {
        std::unique_ptr<HybridCache::CacheEngine> large_item_cache;
        std::unique_ptr<HybridCache::CacheEngine> small_item_cache;
        const uint32_t small_item_max_size{};

        bool isItemLarge(HybridCache::HashedKey key, HybridCache::BufferView value) const
        {
            return key.key().size + value.size() > small_item_max_size;
        }

        void validate();
    };

    struct Config
    {
        std::unique_ptr<HybridCache::Device> device;
        std::unique_ptr<HybridCache::JobScheduler> scheduler;
        std::vector<Pair> pairs;
        // Limited by scheduler parallelism
        UInt32 max_concurrent_inserts{1'000'000};
        UInt64 max_parcel_memory{256 * MiB};
        size_t metadata_size{};

        EnginePairSelector selector{};

        Config & validate();
    };

    explicit NvmCache(Config && config);
    NvmCache(const NvmCache & other) = delete;
    NvmCache & operator=(const NvmCache & other) = delete;
    ~NvmCache() override;

    bool shutDown();

    bool couldExist(HybridCache::HashedKey key, HybridCache::EngineTag tag) override;

    bool isEnabled() const noexcept { return enabled; }

    void disable(const std::string & msg)
    {
        if (isEnabled())
        {
            enabled = false;
            LOG_ERROR(log, "Disable NvmCache {}", msg);
        }
    }

    template <typename T>
    Handle find(HybridCache::HashedKey key, DecodeCallback cb, HybridCache::EngineTag tag);

    using PutToken = typename InFlightPuts::PutToken;
    void put(HybridCache::HashedKey key, std::shared_ptr<void> item, PutToken token, EncodeCallback cb, HybridCache::EngineTag tag);

    PutToken createPutToken(StringRef key);

    using DeleteTombStoneGuard = typename TombStones::Guard;
    void remove(HybridCache::HashedKey key, DeleteTombStoneGuard guard, HybridCache::EngineTag tag);

    DeleteTombStoneGuard createDeleteTombStoneGuard(HybridCache::HashedKey key);

    void reset() override;

    void flush() override;

    void persist() const override;

    bool recover() override;

private:
    Poco::Logger * log = &Poco::Logger::get("NvmCache");

    struct ValidConfigTag
    {
    };
    NvmCache(Config && config, ValidConfigTag);

    std::atomic<bool> enabled{true};

    bool hasTombStone(HybridCache::HashedKey key);

    void removeFromFillMap(HybridCache::HashedKey key)
    {
        std::unique_ptr<GetCtx> to_delete;
        {
            auto lock = getFillLock(key);
            auto & map = getFillMap(key);
            auto it = map.find(key.key());
            if (it == map.end())
                return;
            to_delete = std::move(it->second);
            map.erase(it);
        }
    }

    void invalidateFill(HybridCache::HashedKey key)
    {
        auto shard = getShardForKey(key);
        auto lock = getFillLockForShard(shard);
        auto & map = getFillMapForShard(shard);
        auto it = map.find(key.key());
        if (it != map.end() && it->second)
            it->second->invalidate();
    }

    using FillMap = folly::F14ValueMap<StringRef, std::unique_ptr<GetCtx>>;

    static size_t getShardForKey(HybridCache::HashedKey key) { return key.keyHash() % kShards; }

    static size_t getShardForKey(StringRef key) { return getShardForKey(HybridCache::HashedKey{key}); }

    FillMap & getFillMapForShard(size_t shard) { return fills[shard].fills; }

    FillMap & getFillMap(HybridCache::HashedKey key) { return getFillMapForShard(getShardForKey(key)); }

    std::unique_lock<TimedMutex> getFillLockForShard(size_t shard) { return std::unique_lock<TimedMutex>(fill_locks[shard].fill_lock); }

    std::unique_lock<TimedMutex> getFillLock(HybridCache::HashedKey key) { return getFillLockForShard(getShardForKey(key)); }

    void onGetComplete(
        GetCtx & ctx,
        HybridCache::Status st,
        HybridCache::HashedKey key,
        HybridCache::Buffer val,
        DecodeCallback cb,
        HybridCache::EngineTag tag);

    static constexpr size_t kShards = 8192;

    struct
    {
        alignas(hardware_destructive_interference_size) FillMap fills;
    } fills[kShards];

    struct
    {
        alignas(hardware_destructive_interference_size) TimedMutex fill_lock;
    } fill_locks[kShards];

    std::array<PutContexts, kShards> put_contexts;

    std::array<DelContexts, kShards> del_contexts;

    std::array<InFlightPuts, kShards> inflight_puts;

    std::array<TombStones, kShards> tombstones;

    size_t selectEnginePair(HybridCache::EngineTag tag) const;

    const UInt32 max_concurrent_inserts{};
    const UInt64 max_parcel_memory{};
    const size_t metadata_size{};

    mutable std::atomic<UInt32> concurrent_inserts{};
    mutable std::atomic<UInt64> parcel_memory{};

    std::unique_ptr<HybridCache::Device> device;
    std::unique_ptr<HybridCache::JobScheduler> scheduler;

    const EnginePairSelector selector{};
    std::vector<Pair> pairs;

    bool isItemLarge(HybridCache::HashedKey key, HybridCache::BufferView value, HybridCache::EngineTag tag) const override;

    bool admissionTest(HybridCache::HashedKey key, HybridCache::BufferView value) const;

    static HybridCache::Status
    insertInternal(Pair & pair, HybridCache::HashedKey key, HybridCache::BufferView value, bool & skip_insertion);

    static HybridCache::Status
    lookupInternal(Pair & pair, HybridCache::HashedKey key, HybridCache::Buffer & value, bool & skip_large_item_cache);

    static HybridCache::Status removeHashedKeyInternal(Pair & p, HybridCache::HashedKey key, bool & skip_small_item_cache);

public:
    HybridCache::Status insert(HybridCache::HashedKey key, HybridCache::BufferView value, HybridCache::EngineTag tag) override;
    HybridCache::Status insertAsync(
        HybridCache::HashedKey key, HybridCache::BufferView value, HybridCache::InsertCallback cb, HybridCache::EngineTag tag) override;

    HybridCache::Status lookup(HybridCache::HashedKey key, HybridCache::Buffer & value, HybridCache::EngineTag tag) override;
    void lookupAsync(HybridCache::HashedKey key, HybridCache::LookupCallback cb, HybridCache::EngineTag tag) override;

    HybridCache::Status remove(HybridCache::HashedKey key, HybridCache::EngineTag tag) override;
    void removeAsync(HybridCache::HashedKey key, HybridCache::RemoveCallback cb, HybridCache::EngineTag tag) override;
};
}
