#pragma once

#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <unordered_map>

#include <google/protobuf/io/zero_copy_stream.h>

#include <IO/ReadBufferFromFileBase.h>
#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Allocator.h>
#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Contexts.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/FiberThread.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/InFlightPuts.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/NexusFS/NexusFSBuffer.h>
#include <Storages/NexusFS/NexusFSInodeManager.h>
#include <Common/BitHelpers.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB
{

class NexusFSBufferWithHandle;

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

    // Region size
    UInt64 region_size{1 * MiB};
    UInt64 metadata_size{100 * MiB};
    UInt64 source_buffer_size{DBMS_DEFAULT_BUFFER_SIZE};
    UInt32 segment_size{256 * KiB};
    UInt32 alloc_align_size{4096};
    UInt32 io_align_size{1};
    UInt32 stripe_size{4096};

    UInt32 reader_threads{4};
    UInt32 insert_threads{2};

    UInt32 clean_regions_pool{2};
    UInt32 clean_region_threads{2};

    UInt32 num_in_mem_buffers{4};
    UInt16 in_mem_buf_flush_retry_limit{10};

    UInt16 num_priorities{1};

    bool enable_memory_buffer{false};
    bool support_prefetch{true};
    UInt64 memory_buffer_size{1 * GiB};
    double memory_buffer_cooling_percent{0.1};
    double memory_buffer_freed_percent{0.05};

    UInt32 timeout_ms{10000};
    UInt32 filemeta_gc_interval_s{300};

    String file_prefix;

    // Calculate the total region number.
    UInt32 getNumberRegions() const
    {
        chassert(0ul == metadata_size % region_size);
        chassert(0ul == cache_size % region_size);
        chassert(cache_size > metadata_size);
        return (cache_size - metadata_size) / region_size;
    }

    void loadFromConfig(const Poco::Util::AbstractConfiguration & conf);

private:
    LoggerPtr log = getLogger("NexusFSConfig");

    NexusFSConfig & validate();
    File openFile(const std::string & file_name, UInt64 size, bool truncate, bool direct_io);
};


class NexusFS
{
public:
    struct OffsetAndSize
    {
        off_t offset;
        size_t size;
        OffsetAndSize(off_t offset_, size_t size_) : offset(offset_), size(size_) { }
    };
    using OffsetAndSizeVector = std::vector<OffsetAndSize>;

    struct InsertCxt
    {
        std::mutex mutex;
        std::condition_variable cv;
        HybridCache::Buffer buffer;
        bool ready = false;
    };

    explicit NexusFS(NexusFSConfig && config);
    NexusFS(const NexusFS &) = delete;
    NexusFS & operator=(const NexusFS &) = delete;
    ~NexusFS();


    UInt64 getSize() const { return region_manager.getSize(); }
    UInt64 getSegmentSize() const { return segment_size; }
    bool supportNonCopyingRead() const { return enable_buffer; }
    bool supportPrefetch() const { return support_prefetch; }
    String getFilePrefix() const { return file_prefix; }
    UInt64 getNumSegments() const { return num_segments.load(); }
    UInt64 getNumInflightInserts() const { return num_inflight_inserts.load(); }
    UInt64 getNumInodes() const { return index.getNumInodes(); }
    UInt64 getNumFileMetas() const { return index.getNumFileMetas(); }
    std::vector<NexusFSComponents::FileCachedState> getFileCachedStates() { return index.getFileCachedStates(); }

    HybridCache::FiberThread & getReadWorker()
    {
        return *(reader_workers[reader_task_counter.fetch_add(1, std::memory_order_relaxed) % reader_workers.size()]);
    }

    // preload an array of segments to disk cache
    void preload(const String & file, const OffsetAndSizeVector & offsets_and_sizes, std::unique_ptr<ReadBufferFromFileBase> & source);

    // read from nexusfs
    size_t read(const String & file, off_t offset, size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source, char * to);

    // read from nexusfs (non-copy)
    NexusFSBufferWithHandle read(const String & file, off_t offset, size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source);

    std::future<NexusFSBufferWithHandle>
    prefetchToBuffer(const String & file, off_t offset, size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source);

    void flush();
    void drain();
    bool shutDown();
    void reset();
    void persist();
    bool recover();

private:
    class InFlightInserts
    {
    public:
        std::pair<std::shared_ptr<InsertCxt>, bool> getOrCreateContext(const String & file_and_segment_id)
        {
            auto shard = std::hash<String>()(file_and_segment_id) % kShards;
            auto & mutex = mutexs[shard];
            auto & map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                auto it = map.find(file_and_segment_id);
                if (it != map.end())
                    return std::make_pair(it->second, false);
                auto cxt = std::make_shared<InsertCxt>();
                map[file_and_segment_id] = cxt;
                return std::make_pair(cxt, true);
            }
        }

        std::shared_ptr<InsertCxt> getContext(const String & file_and_segment_id)
        {
            auto shard = std::hash<String>()(file_and_segment_id) % kShards;
            auto & mutex = mutexs[shard];
            auto & map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                auto it = map.find(file_and_segment_id);
                if (it != map.end())
                    return it->second;
                return nullptr;
            }
        }

        void removeContext(const String & file_and_segment_id)
        {
            auto shard = std::hash<String>()(file_and_segment_id) % kShards;
            auto & mutex = mutexs[shard];
            auto & map = maps[shard];
            {
                std::lock_guard<std::mutex> guard{mutex};
                map.erase(file_and_segment_id);
            }
        }

    private:
        static constexpr UInt32 kShards = 8192;
        std::array<std::mutex, kShards> mutexs;
        std::array<std::unordered_map<String, std::shared_ptr<InsertCxt>>, kShards> maps;
    };


    static constexpr UInt32 kFormatVersion = 15;
    static constexpr UInt16 kDefaultItemPriority = 0;

    UInt64 getSegmentId(const off_t offset) const { return offset / segment_size; }
    static String getSegmentName(const String file, const UInt64 segment_id) { return file + "#" + std::to_string(segment_id); }
    static String getSourceSegmentName(const String file, const UInt64 segment_id) { return file + "@" + std::to_string(segment_id); }
    off_t getOffsetInSourceFile(const UInt64 segment_id) const { return segment_id * segment_size; }
    off_t getOffsetInSegment(const off_t file_offset) const { return file_offset % segment_size; }
    off_t getOffsetInSourceBuffer(const off_t file_offset) const { return file_offset % source_buffer_size; }
    static size_t getReadSizeInSegment(const off_t offset_in_segemt, const size_t segment_size, const size_t buffer_size)
    {
        return segment_size >= static_cast<size_t>(offset_in_segemt) ? std::min(buffer_size, segment_size - offset_in_segemt) : 0;
    }
    UInt32 alignedSize(UInt32 size) const { return roundup(size, alloc_align_size); }
    HybridCache::FiberThread & getInsertWorker()
    {
        return *(insert_workers[insert_task_counter.fetch_add(1, std::memory_order_relaxed) % insert_workers.size()]);
    }

    std::tuple<std::shared_ptr<NexusFSComponents::BlockHandle>, std::shared_ptr<InsertCxt>, UInt64> open(
        const String & segment_name,
        const String & file,
        UInt64 segment_id,
        std::unique_ptr<ReadBufferFromFileBase> & source);

    std::pair<NexusFSComponents::OpResult, size_t>
    readFromInsertCxtInternal(std::shared_ptr<InsertCxt> & cxt, off_t offset_in_segment, size_t max_size, char * to) const;
    std::pair<NexusFSComponents::OpResult, NexusFSBufferWithHandle>
    readFromInsertCxtInternal(std::shared_ptr<InsertCxt> & cxt, off_t offset_in_segment, size_t max_size) const;
    std::pair<NexusFSComponents::OpResult, size_t> readFromBufferInternal(
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle, UInt64 seq_number, off_t offset_in_segment, size_t size, char * to);
    std::pair<NexusFSComponents::OpResult, NexusFSBufferWithHandle> readFromBufferInternal(
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle, UInt64 seq_number, off_t offset_in_segment, size_t size);
    std::pair<NexusFSComponents::OpResult, size_t> readFromDiskInternal(
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle, UInt64 seq_number, off_t offset_in_segment, size_t size, char * to);

    // read form insert_cxt
    std::pair<bool, size_t> readFromInsertCxt(
        Stopwatch & watch,
        const String & segment_name,
        std::shared_ptr<InsertCxt> & cxt,
        off_t offset_in_segment,
        size_t max_size,
        char * to) const;
    // read form insert_cxt (non-copy)
    std::pair<bool, NexusFSBufferWithHandle> readFromInsertCxt(
        Stopwatch & watch, const String & segment_name, std::shared_ptr<InsertCxt> & cxt, off_t offset_in_segment, size_t max_size) const;
    // read from buffer
    std::pair<bool, size_t> readFromBuffer(
        Stopwatch & watch,
        const String & segment_name,
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
        UInt64 seq_number,
        off_t offset_in_segment,
        size_t size,
        char * to);
    // read from buffer (non-copy)
    std::pair<bool, NexusFSBufferWithHandle> readFromBuffer(
        Stopwatch & watch,
        const String & segment_name,
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
        UInt64 seq_number,
        off_t offset_in_segment,
        size_t size);
    // read from disk
    std::pair<bool, size_t> readFromDisk(
        Stopwatch & watch,
        const String & segment_name,
        std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
        UInt64 seq_number,
        off_t offset_in_segment,
        size_t size,
        char * to);


    void writeEntry(std::shared_ptr<NexusFSComponents::BlockHandle> & handle, HybridCache::BufferView value);

    size_t readEntry(const HybridCache::RegionDescriptor & desc, HybridCache::RelAddress addr, UInt32 size, char * to);

    UInt32 onRegionReclaim(HybridCache::RegionId rid, HybridCache::BufferView buffer);

    void onRegionCleanup(HybridCache::RegionId rid, HybridCache::BufferView buffer);

    static Protos::NexusFSConfig serializeConfig(const NexusFSConfig & config);

    enum class ReinsertionRes
    {
        kReinserted,
        kRemoved,
        kEvicted,
    };
    ReinsertionRes reinsertOrRemoveItem(std::shared_ptr<NexusFSComponents::BlockHandle> & handle);

    void removeItem(std::shared_ptr<NexusFSComponents::BlockHandle> & handle);

    std::shared_ptr<NexusFSComponents::BlockHandle> insert(
        const String & file,
        UInt64 segment_id,
        HybridCache::BufferView buf_view,
        std::function<std::pair<size_t, UInt32>()> get_file_and_segment_size);


    LoggerPtr log = getLogger("NexusFS");

    const Protos::NexusFSConfig serialized_config;

    // const UInt16 num_priorities;
    // const HybridCache::ExpiredCheck check_expired;
    // const HybridCache::DestructorCallback destructor_callback;
    // const bool checksum_data{};
    // const bool item_destructor_enabled{false};
    // const bool precise_remove{false};

    const std::unique_ptr<HybridCache::Device> device;
    const UInt32 alloc_align_size{};
    const UInt64 metadata_size{};
    const UInt64 source_buffer_size{};
    const UInt32 segment_size{};
    const UInt32 num_segments_per_source_buffer{};
    const UInt32 timeout_ms{};

    const String file_prefix;
    NexusFSComponents::InodeManager index;
    HybridCache::RegionManager region_manager;
    HybridCache::Allocator allocator;

    InFlightInserts in_flight_inserts;

    const bool enable_buffer;
    const bool support_prefetch;
    NexusFSComponents::BufferManager * buffer_manager;
    std::vector<std::unique_ptr<HybridCache::FiberThread>> reader_workers;
    std::vector<std::unique_ptr<HybridCache::FiberThread>> insert_workers;
    mutable std::atomic<UInt64> reader_task_counter{0};
    mutable std::atomic<UInt64> insert_task_counter{0};

    std::atomic<UInt64> num_segments{0};
    std::atomic<UInt64> num_inflight_inserts{0};
};
}
