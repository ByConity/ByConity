#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>
#include <Common/thread_local_rng.h>
#include <common/types.h>

namespace DB::HybridCache
{
// Callback that is used to clear index.
using RegionEvictCallback = std::function<UInt32(RegionId rid, BufferView buffer)>;

// Callback that is used to clean up region.
using RegionCleanupCallback = std::function<void(RegionId rid, BufferView buffer)>;

// Size class or stack allocator.
class RegionManager
{
public:
    RegionManager(
        UInt32 num_regions_,
        UInt64 region_size_,
        UInt64 base_offset_,
        Device & device_,
        UInt32 num_clean_regions_,
        JobScheduler & scheduler_,
        RegionEvictCallback evict_callback_,
        RegionCleanupCallback cleanup_callback_,
        std::unique_ptr<EvictionPolicy> policy_,
        UInt32 num_in_mem_buffers_,
        UInt16 num_priorities_,
        UInt16 in_mem_buf_flush_retry_limit_);
    RegionManager(const RegionManager &) = delete;
    RegionManager & operator=(const RegionManager &) = delete;

    // Returns the size of usable space.
    UInt64 getSize() const { return static_cast<UInt64>(num_regions) * region_size; }

    // GVets a region from a valid region ID.
    Region & getRegion(RegionId rid)
    {
        chassert(rid.valid());
        return *regions[rid.index()];
    }

    // Gets a const region from a valid region ID.
    const Region & getRegion(RegionId rid) const
    {
        chassert(rid.valid());
        return *regions[rid.index()];
    }

    RegionId getRandomRegion() const
    {
        std::uniform_int_distribution<UInt32> dist(0, num_regions - 1);
        return RegionId{dist(thread_local_rng)};
    }

    // Flushes the in memory buffer attached to a region.
    void doFlush(RegionId rid, bool async);

    // Returns the size of one region.
    UInt64 regionSize() const { return region_size; }

    // Gets a region to evict.
    RegionId evict();

    // Promote a region.
    void touch(RegionId rid);

    void track(RegionId rid);

    // Resets all region internal state.
    void reset();

    // Atomically loads the current sequence nunber.
    UInt64 getSeqNumber() const { return seq_number.load(std::memory_order_acquire); }

    AbsAddress toAbsolute(RelAddress addr) const { return AbsAddress{addr.offset() + addr.rid().index() * region_size}; }

    RelAddress toRelative(AbsAddress addr) const
    {
        return RelAddress{RegionId(addr.offset() / region_size), static_cast<UInt32>(addr.offset() % region_size)};
    }

    // Assigns a buffer from buffer pool;
    std::unique_ptr<Buffer> claimBufferFromPool();

    // Returns the buffer to the pool.
    void returnBufferToPool(std::unique_ptr<Buffer> buf);

    // Writes buffer at addr.
    void write(RelAddress addr, Buffer buf);

    // Retruns a buffer with data read from the device.
    Buffer read(const RegionDescriptor & desc, RelAddress addr, size_t size) const;

    // Flushes all in memory buffers to the device.
    void flush();

    // Flushes the in memory buffer attached to a region.
    Region::FlushRes flushBuffer(const RegionId & rid);

    // Detaches the buffer from the region and returns the buffer to pool.
    bool detachBuffer(const RegionId & rid);

    // Cleans up the in memory buffer when flushing failure reach the retry limit.
    bool cleanupBufferOnFlushFailure(const RegionId & rid);

    // Releases a region taht was cleaned up due to in-mem buffer flushing failure.
    void releaseCleanedupRegion(RegionId rid);

    void persist(google::protobuf::io::CodedOutputStream * stream) const;

    void recover(google::protobuf::io::CodedInputStream * stream);

    // Opens a region for reading and returns the region descriptor.
    RegionDescriptor openForRead(RegionId rid, UInt64 seq_number_);

    // Closes the region and consume the region descriptor.
    void close(RegionDescriptor && desc);

    // Fetches a clean region from the list and schedules reclaim jobs to refill the list.
    OpenStatus getCleanRegion(RegionId & rid);

    // Tries to get a free region first, otherwise evicts one and schedules region cleanup job.
    void startReclaim();

    // Releases a region that was evicted during region reclamation.
    void releaseEvictedRegion(RegionId rid, std::chrono::nanoseconds start_time);

    // Evicts a region during region reclamation.
    void doEviction(RegionId rid, BufferView buffer) const;


private:
    Poco::Logger * log = &Poco::Logger::get("RegionManager");

    using LockGuard = std::lock_guard<std::mutex>;
    UInt64 physicalOffset(RelAddress addr) const { return base_offset + toAbsolute(addr).offset(); }

    bool deviceWrite(RelAddress addr, BufferView buf);

    bool isValidIORange(UInt32 offset, UInt32 size) const;
    OpenStatus assignBufferToRegion(RegionId rid);

    // Initializes the eviction policy.
    void resetEvictionPolicy();

    const UInt16 num_priorities{};
    const UInt16 in_mem_buf_flush_retry_limit{};
    const UInt32 num_regions{};
    const UInt64 region_size{};
    const UInt64 base_offset{};
    Device & device;
    const std::unique_ptr<EvictionPolicy> policy;
    std::unique_ptr<std::unique_ptr<Region>[]> regions;

    mutable std::mutex clean_regions_mutex;
    std::vector<RegionId> clean_regions;
    const UInt32 num_clean_regions{};

    std::atomic<UInt64> seq_number{0};

    UInt32 reclaim_scheduled{0};
    JobScheduler & scheduler;

    const RegionEvictCallback evict_callback;
    const RegionCleanupCallback cleanup_callback;

    const UInt32 num_in_mem_buffers{0};

    mutable std::mutex buffer_mutex;
    std::vector<std::unique_ptr<Buffer>> buffers;
};
}
