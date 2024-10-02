#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <unordered_map>

#include <google/protobuf/io/zero_copy_stream.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Allocator.h>
#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/Contexts.h>
#include <Storages/DiskCache/InFlightPuts.h>
#include <Storages/NexusFS/NexusFSIndex.h>
#include <Common/BitHelpers.h>
#include <common/defines.h>
#include <common/types.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

class NexusFSConfig
{
public:
    static constexpr std::string_view CONFIG_NAME = "nexus_fs";
    static constexpr std::string_view FILE_NAME = "nexus_fs.bin";

    std::vector<String> file_paths;
    std::vector<File> file_vec;

    std::unique_ptr<HybridCache::Device> device;
    // HybridCache::ExpiredCheck check_expired;
    // HybridCache::DestructorCallback destructor_callback;
    // bool checksum{};
    // bool item_destructor_enabled{false};

    UInt64 cache_size{10 * GiB};

    std::unique_ptr<HybridCache::EvictionPolicy> eviction_policy;
    BlockCacheReinsertionConfig reinsertion_config{};

    // Region size
    UInt64 region_size{1 * MiB};
    UInt64 metadata_size{100 * MiB};
    UInt32 segment_size{256 * KiB};
    UInt32 alloc_align_size{4096};
    UInt32 io_align_size{1};
    UInt32 stripe_size{4096};

    UInt32 clean_regions_pool{2};
    UInt32 clean_region_threads{2};

    UInt32 num_in_mem_buffers{4};
    UInt16 in_mem_buf_flush_retry_limit{10};

    UInt16 num_priorities{1};

    UInt64 memory_cache_size{1 * GiB};

    UInt32 timeout_ms{10000};

    // Calculate the total region number.
    UInt32 getNumberRegions() const
    {
        chassert(0ul == cache_size % region_size);
        return cache_size / region_size;
    }

    void loadFromConfig(const Poco::Util::AbstractConfiguration & conf);

private:
    Poco::Logger * log = &Poco::Logger::get("NexusFSConfig");

    NexusFSConfig & validate();
    File openFile(const std::string & file_name, UInt64 size, bool truncate);
};


class NexusFS
{
public:
    struct OffsetAndSize
    {
        off_t offset;
        size_t size;
        OffsetAndSize(off_t offset_, size_t size_) : offset(offset_), size(size_) {}
    };
    using OffsetAndSizeVector = std::vector<OffsetAndSize>;

    explicit NexusFS(NexusFSConfig && config);
    NexusFS(const NexusFS &) = delete;
    NexusFS & operator=(const NexusFS &) = delete;
    ~NexusFS() = default;


    UInt64 getSize() const { return region_manager.getSize(); }
    UInt64 getSegmentSize() const { return segment_size; }


    void preload(const String &file, const OffsetAndSizeVector &offsets_and_sizes, std::unique_ptr<ReadBufferFromFileBase> &source);
    size_t read(const String &file, off_t offset, size_t max_size, std::unique_ptr<ReadBufferFromFileBase> &source, char *to);


    void flush();
    void drain();
    bool shutDown();
    void reset();
    void persist();
    bool recover();

private:

    struct InsertCxt
    {
        HybridCache::Buffer buffer;
        bool ready = false; //TODO: use waiter
    };

    class InFlightInserts
    {
    public:
        std::shared_ptr<InsertCxt> getOrCreateContext(UInt32 key, bool &is_newly_created)
        {
            auto shard = key % kShards;
            auto &mutex = mutexs[shard];
            auto &map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                auto it = map.find(key);
                if (it != map.end())
                {
                    is_newly_created = false;
                    return it->second;
                }
                is_newly_created = true;
                auto cxt = std::make_shared<InsertCxt>();
                map[key] = cxt;
                return cxt;
            }
        }

        std::shared_ptr<InsertCxt> getContext(UInt32 key)
        {
            auto shard = key % kShards;
            auto &mutex = mutexs[shard];
            auto &map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                auto it = map.find(key);
                if (it != map.end())
                    return it->second;
                return nullptr;
            }
        }

        void removeContext(UInt32 key)
        {
            auto shard = key % kShards;
            auto &mutex = mutexs[shard];
            auto &map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                map.erase(key);
            }
        }

    private:
        static constexpr UInt32 kShards = 8192;
        std::array<std::mutex, kShards> mutexs;
        std::array<std::unordered_map<UInt64, std::shared_ptr<InsertCxt>>, kShards> maps;
    };


    static constexpr UInt32 kFormatVersion = 15;
    static constexpr UInt16 kDefaultItemPriority = 0;

    UInt64 getSegmentId(const off_t offset) const { return offset / segment_size; }
    static String getSegmentName(const String file, const UInt64 segment_id)  { return file + "#" + std::to_string(segment_id); }
    off_t getOffsetInSourceFile(const UInt64 segment_id) const { return segment_id * segment_size; }
    off_t getOffsetInSegment(const off_t file_offset) const { return file_offset % segment_size; }
    static size_t getReadSizeInSegment(const off_t offset_in_segemt, const size_t segment_size, const size_t buffer_size) { return std::min(buffer_size, segment_size - offset_in_segemt); }
    UInt32 alignedSize(UInt32 size) const { return roundup(size, alloc_align_size); }

    NexusFSComponents::NexusFSIndex::LookupResult load(const HybridCache::HashedKey &key, off_t offset_in_source, std::unique_ptr<ReadBufferFromFileBase> &source, std::shared_ptr<InsertCxt> &insert_cxt);
    
    void writeEntry(HybridCache::RelAddress addr, UInt32 slot_size, const HybridCache::HashedKey &key, HybridCache::BufferView value);

    size_t readEntry(const HybridCache::RegionDescriptor &desc, HybridCache::RelAddress addr, UInt32 size, char *to);

    UInt32 onRegionReclaim(HybridCache::RegionId rid, HybridCache::BufferView buffer);

    void onRegionCleanup(HybridCache::RegionId rid, HybridCache::BufferView buffer);

    static Protos::NexusFSConfig serializeConfig(const NexusFSConfig & config);

    enum class ReinsertionRes
    {
        kReinserted,
        kRemoved,
        kEvicted,
    };
    ReinsertionRes reinsertOrRemoveItem(UInt64 key, HybridCache::BufferView value, UInt32 entry_size, HybridCache::RelAddress addr);

    bool removeItem(UInt64 key, HybridCache::RelAddress addr);

    std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> makeReinsertionPolicy(const BlockCacheReinsertionConfig & reinsertion_config);

    NexusFSComponents::NexusFSIndex::LookupResult insert(const HybridCache::HashedKey &key, HybridCache::BufferView buf_view);

    // std::shared_ptr<NexusFSComponents::SegmentHandle> loadToMemoryCache(const NexusFSComponents::NexusFSIndex::LookupResult &lr);


    Poco::Logger * log = &Poco::Logger::get("NexusFS");

    const Protos::NexusFSConfig serialized_config;

    // const UInt16 num_priorities;
    // const HybridCache::ExpiredCheck check_expired;
    // const HybridCache::DestructorCallback destructor_callback;
    // const bool checksum_data{};
    // const bool item_destructor_enabled{false};
    // const bool precise_remove{false};

    const std::unique_ptr<HybridCache::Device> device;
    const UInt32 alloc_align_size{};
    const UInt64 region_size{};
    const UInt64 metadata_size{};
    const UInt32 segment_size{};
    const UInt32 timeout_ms{};

    NexusFSComponents::NexusFSIndex index;
    HybridCache::RegionManager region_manager;
    HybridCache::Allocator allocator;

    std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> reinsertion_policy;

    InFlightInserts in_flight_inserts;

    const bool enable_segment_cache;
    // NexusFSComponents::SegmentCacheLRU segment_cache;
};
}
