#pragma once

#include <common/types.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
namespace CurrentMemoryTracker
{
    extern UInt8 g_total_untracked_memory_limit_encoded;
    constexpr UInt8 DEFAULT_TOTAL_UNTRACKED_LIMIT_ENCODED{0xc1};
    void alloc(Int64 size);
    void allocNoThrow(Int64 size);
    void realloc(Int64 old_size, Int64 new_size);
    void free(Int64 size);
    void check();
}
