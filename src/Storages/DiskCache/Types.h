#pragma once

#include <Storages/DiskCache/HashKey.h>

namespace DB::HybridCache
{
enum class EngineTag : UInt32
{
    UncompressedCache = 0,
    // keep count at last
    COUNT,
};

inline StringRef getEngineTagName(EngineTag t)
{
    switch (t)
    {
        case EngineTag::UncompressedCache:
            return "uncompressed_cache";
        default:
            return "invalid";
    }
    chassert(false);
    return "invalid";
}

enum class Status
{
    Ok,

    // Entry not found
    NotFound,

    // Operations were rejected (queue full, admission policy, etc.)
    Rejected,

    // Resource is temporary busy
    Retry,

    // Device IO error or out of memory
    DeviceError,

    // Internal invariant broken. Consistency may be violated.
    BadState,
};

enum class DestructorEvent
{
    // space is recycled (item evicted)
    Recycled,
    // item is removed from NVM
    Removed,
    // item already in the queue but failed to put into NVM
    PutFailed,
};

using DestructorCallback = std::function<void(HashedKey key, BufferView value, DestructorEvent event)>;

// Checking NvmItem expired
using ExpiredCheck = std::function<bool(BufferView value)>;

enum class IoEngine : uint8_t
{
    IoUring,
    Sync
};

inline StringRef getIoEngineName(IoEngine e)
{
    switch (e)
    {
        case IoEngine::IoUring:
            return "io_uring";
        case IoEngine::Sync:
            return "sync";
    }
    chassert(false);
    return "invalid";
}

class RegionId
{
public:
    RegionId() = default;
    explicit RegionId(UInt32 idx_) : idx(idx_) { }

    bool valid() const noexcept { return idx != kInvalid; }

    UInt32 index() const noexcept { return idx; }

    bool operator==(const RegionId & other) const noexcept { return idx == other.idx; }

    bool operator!=(const RegionId & other) const noexcept { return !(*this == other); }

private:
    static constexpr UInt32 kInvalid{~0u};
    UInt32 idx{kInvalid};
};

/**
 * Device address as a flat 64-bit byte offset
 */
class AbsAddress
{
public:
    AbsAddress() = default;
    explicit AbsAddress(UInt64 o) : off{o} { }

    UInt64 offset() const { return off; }

    AbsAddress add(UInt64 ofs) const { return AbsAddress{off + ofs}; }


    AbsAddress sub(UInt64 ofs) const
    {
        chassert(ofs <= off);
        return AbsAddress{off - ofs};
    }

    bool operator==(AbsAddress other) const { return off == other.off; }
    bool operator!=(AbsAddress other) const { return !(*this == other); }

private:
    UInt64 off{};
};

/**
 *  Device address as 32-bit region id and 32-bit offset
 *  inside that region.
 */
class RelAddress
{
public:
    RelAddress() = default;
    explicit RelAddress(RegionId r, UInt32 o = 0) : region_id{r}, off{o} { }

    RegionId rid() const { return region_id; }

    UInt32 offset() const { return off; }

    RelAddress add(UInt32 ofs) const { return RelAddress{region_id, off + ofs}; }

    RelAddress sub(UInt32 ofs) const
    {
        chassert(ofs <= off);
        return RelAddress{region_id, off - ofs};
    }

    bool operator==(RelAddress other) const { return off == other.off && region_id == other.region_id; }
    bool operator!=(RelAddress other) const { return !(*this == other); }

private:
    RegionId region_id;
    UInt32 off{};
};

constexpr UInt32 kMaxKeySize{255};
}
