#pragma once

#include <memory>
#include <vector>

#include <folly/fibers/TimedMutex.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/ConditionVariable.h>
#include <Storages/DiskCache/Types.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::NexusFSComponents
{
class BlockHandle;
}

namespace DB::HybridCache
{

using folly::fibers::TimedMutex;

enum class OpenMode
{
    // Not opened
    None,
    // Write Mode
    Write,
    // Read Mode
    Read,
};

enum class OpenStatus
{
    // Can Proceed
    Ready,

    // Sequence number mismatch
    Retry,

    // Error
    Error,
};

class RegionDescriptor;

// Region responsible for open/lock synchronization and keeps count of active readers/writers.
class Region
{
public:
    Region(RegionId id, UInt64 region_size_) : region_id{id}, region_size{region_size_} { }

    Region(const Protos::Region & region_, UInt64 region_size_)
        : region_id{region_.region_id()}
        , region_size{region_size_}
        , priority{static_cast<UInt16>(region_.priority())}
        , last_entry_end_offset{region_.last_entry_end_offset()}
        , num_items{region_.num_items()}
    {
    }

    Region(const Region &) = delete;
    Region & operator=(const Region &) = delete;

    // Immediately block future access to this region. Return true if there are no pending operation
    // to this region, false otherwise.
    bool readyForReclaim(bool wait);

    // Open this region for write and allocate a slot of size.
    // Fail if there's insufficient space.
    std::tuple<RegionDescriptor, RelAddress> openAndAllocate(UInt32 size);

    // Opens this region for reading. Fail if region is blocked.
    RegionDescriptor openForRead();

    // Resets the region's internal state.
    void reset();

    // Closes the region and consume the region descriptor.
    void close(RegionDescriptor && desc);

    // Assigns the region a priority.
    void setPriority(UInt16 priority_)
    {
        std::lock_guard<TimedMutex> g{lock};
        priority = priority_;
    }

    // Gets the proiority this region assigned.
    UInt16 getPriority() const
    {
        std::lock_guard<TimedMutex> g{lock};
        return priority;
    }

    // Gets the end offset of last slot added to this region.
    UInt32 getLastEntryEndOffset() const
    {
        std::lock_guard<TimedMutex> g{lock};
        return last_entry_end_offset;
    }

    // Gets the number of items in this region.
    UInt32 getNumItems() const
    {
        std::lock_guard<TimedMutex> g{lock};
        return num_items;
    }

    // if this region is actively used, then the fragmentation is the bytes at the end of the region that's not used.
    UInt32 getFragmentationSize() const
    {
        std::lock_guard<TimedMutex> g{lock};
        if (num_items)
            return region_size - last_entry_end_offset;
        return 0;
    }

    // Writes buf to attached buffer at offset.
    void writeToBuffer(UInt32 offset, BufferView buf);

    // Reads from attached buffer from offset.
    void readFromBuffer(UInt32 from_offset, MutableBufferView out_buf) const;
    void readFromBuffer(UInt32 from_offset, size_t size, char *to) const;

    // Attach buffer to the region.
    void attachBuffer(std::unique_ptr<Buffer> && buf)
    {
        std::lock_guard g{lock};
        chassert(buffer == nullptr);
        buffer = std::move(buf);
    }

    // Check if tthe region has buffer attached.
    bool hasBuffer() const
    {
        std::lock_guard g{lock};
        return buffer != nullptr;
    }

    // Detaches the attached buffer and returns it only if there are no active reads, otherwise returns nullptr.
    std::unique_ptr<Buffer> detachBuffer();

    // Flushes the attached buffer by calling the callBack function.
    enum FlushRes
    {
        kSuccess,
        kRetryDeviceFailure,
        kRetryPendingWrites,
    };
    FlushRes flushBuffer(std::function<bool(RelAddress, BufferView)> callback);

    // Cleans up the attached buffer.
    void cleanupBuffer(std::function<void(RegionId, BufferView)> callback);

    // Marks the bit to indicate pending flush status.
    void setPendingFlush()
    {
        std::lock_guard g{lock};
        chassert(buffer != nullptr);
        chassert((flags & (kFlushedPending | kFlushed)) == 0);

        flags |= kFlushedPending;
    }

    // Checks if the region's buffer is flushed.
    bool isFlushedLocked() const { return (flags & kFlushed) != 0; }

    // Check whether the region's buffer is cleanup.
    bool isCleanedupLocked() const { return (flags & kCleanedup) != 0; }

    // Returns the number of active writers using the region.
    UInt32 getActiveWriters() const
    {
        std::lock_guard g{lock};
        return active_writers;
    }

    // Returns the number of active readers using the region.
    UInt32 getActiveInMemReaders() const
    {
        std::lock_guard g{lock};
        return active_in_mem_readers;
    }

    // Returns the region id.
    RegionId id() const { return region_id; }

    void addHandle(std::shared_ptr<NexusFSComponents::BlockHandle> &handle);
    void resetHandles();
    void getHandles(std::vector<std::shared_ptr<NexusFSComponents::BlockHandle>> &handles_);

private:
    UInt32 activeOpenLocked() const;

    // check if there is enough space in the region for a new write of size.
    bool canAllocateLocked(UInt32 size) const
    {
        chassert((flags & (kFlushedPending | kFlushed)) == 0);
        return (last_entry_end_offset + size <= region_size);
    }

    RelAddress allocateLocked(UInt32 size);

    static constexpr UInt32 kBlockAccess{1u << 0};
    static constexpr UInt16 kPinned{1u << 1};
    static constexpr UInt16 kFlushedPending{1u << 2};
    static constexpr UInt16 kFlushed{1u << 3};
    static constexpr UInt16 kCleanedup{1u << 4};

    const RegionId region_id{};
    const UInt64 region_size{0};

    UInt16 priority{0};
    UInt16 flags{0};
    UInt32 active_phys_readers{0};
    UInt32 active_in_mem_readers{0};
    UInt32 active_writers{0};

    UInt32 last_entry_end_offset{0};
    UInt32 num_items{0};
    std::unique_ptr<Buffer> buffer{nullptr};

    std::vector<std::shared_ptr<NexusFSComponents::BlockHandle>> handles;

    mutable TimedMutex lock{TimedMutex::Options(false)};
    mutable ConditionVariable cond;
};

// RegionDescriptor contains status of the open, region id and the open mode.
class RegionDescriptor
{
public:
    explicit RegionDescriptor(OpenStatus status_) : status(status_) { }
    static RegionDescriptor makeWriteDescriptor(OpenStatus status_, RegionId region_id_)
    {
        return RegionDescriptor{status_, region_id_, OpenMode::Write};
    }
    static RegionDescriptor makeReadDescriptor(OpenStatus status_, RegionId region_id_, bool phys_read_mode_)
    {
        return RegionDescriptor{status_, region_id_, OpenMode::Read, phys_read_mode_};
    }
    RegionDescriptor(const RegionDescriptor &) = delete;
    RegionDescriptor & operator=(const RegionDescriptor &) = delete;

    RegionDescriptor(RegionDescriptor && o) noexcept
        : status{o.status}, region_id{o.region_id}, mode{o.mode}, phys_read_mode{o.phys_read_mode}
    {
        o.mode = OpenMode::None;
        o.region_id = RegionId{};
        o.status = OpenStatus::Retry;
        o.phys_read_mode = false;
    }

    RegionDescriptor & operator=(RegionDescriptor && o) noexcept
    {
        if (this != &o)
        {
            this->~RegionDescriptor();
            new (this) RegionDescriptor(std::move(o));
        }
        return *this;
    }

    // Check whether the current  open mode is physical read mode.
    bool isPhysReadMode() const { return (mode == OpenMode::Read) && phys_read_mode; }

    // Checks whether status of the open is ready.
    bool isReady() const { return status == OpenStatus::Ready; }

    // Return the current open mode.
    OpenMode getMode() const { return mode; }

    // Return the current open status.
    OpenStatus getStatus() const { return status; }

    // Return the unique region ID.
    RegionId id() const { return region_id; }

private:
    RegionDescriptor(OpenStatus status_, RegionId region_id_, OpenMode mode_, bool phys_read_mode_ = false)
        : status(status_), region_id(region_id_), mode(mode_), phys_read_mode(phys_read_mode_)
    {
    }
    OpenStatus status;
    RegionId region_id{};
    OpenMode mode{OpenMode::None};
    // phys_read_mode is applicable only in read mode.
    bool phys_read_mode{false};
};
}
