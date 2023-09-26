#pragma once

#include <Storages/DiskCache/HashKey.h>

namespace DB::HybridCache
{
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

using DestructorCallback = std::function<void(HashedKey hk, BufferView value, DestructorEvent event)>;

// Checking NvmItem expired
using ExpiredCheck = std::function<bool(BufferView value)>;

enum class IoEngine : uint8_t
{
    IoUring,
    LibAio,
    Sync
};

inline StringRef getIoEngineName(IoEngine e)
{
    switch (e)
    {
        case IoEngine::IoUring:
            return "io_uring";
        case IoEngine::LibAio:
            return "libaio";
        case IoEngine::Sync:
            return "sync";
    }
    chassert(false);
    return "invalid";
}

}
