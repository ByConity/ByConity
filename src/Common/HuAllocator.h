#pragma once
#include <string.h>

#ifdef NDEBUG
    #define ALLOCATOR_ASLR 0
#else
    #define ALLOCATOR_ASLR 1
#endif

#include <pcg_random.hpp>
#include <Common/thread_local_rng.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <cstdlib>
#include <algorithm>
#include <sys/mman.h>

#include <Core/Defines.h>
#include <common/getPageSize.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <common/errnoToString.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <Common/config.h>

#if USE_HUALLOC
#include <hualloc/hu_alloc.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

}
static constexpr size_t HUMALLOC_MIN_ALIGNMENT = 8;

template <bool clear_memory_>
class HuAllocator
{
public:

    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0)
    {
        checkSize(size);
        CurrentMemoryTracker::alloc(size);
        void * ptr = allocNoTrack(size, alignment);
        return ptr;
    }

    /// Free memory range.
    void free(void * buf, size_t size)
    {
        try
        {
            checkSize(size);
            freeNoTrack(buf);
            CurrentMemoryTracker::free(size);
        }
        catch (...)
        {
            DB::tryLogCurrentException("HugeAllocator::free");
            throw;
        }
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        checkSize(new_size);

        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else if (alignment <= HUMALLOC_MIN_ALIGNMENT)
        {
            /// Resize malloc'd memory region with no special alignment requirement.
            CurrentMemoryTracker::alloc(new_size);
            void * new_buf = hu_realloc(buf, new_size);
            if (nullptr == new_buf)
            {
                DB::throwFromErrno(
                    fmt::format("HugeAllocator: Cannot realloc from {} to {}.", ReadableSize(old_size), ReadableSize(new_size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
            }

            buf = new_buf;
            CurrentMemoryTracker::free(old_size);
            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
        }
        else
        {
            /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.
            void * new_buf = alloc(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free(buf, old_size);
            buf = new_buf;
        }

        return buf;
    }

    static void InitHuAlloc(size_t cached)
    {
        static std::once_flag hualloc_init_flag;
        static size_t use_cache = cached / 2;
        if (use_cache <= 0)
            use_cache = 1024 * (1ull << 20); /// If not set properly use 1G as default

        std::call_once(hualloc_init_flag, [&]()
        {
            hu_check_init_w();
            pthread_t tid;
            pthread_create(&tid, nullptr, ReclaimThread, &use_cache);
        });
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = clear_memory_;

private:

    void * allocNoTrack(size_t size, size_t alignment)
    {
        void * buf;
        if (alignment <= HUMALLOC_MIN_ALIGNMENT)
        {
            if constexpr (clear_memory)
                buf = hu_calloc(size, 1);
            else
                buf = hu_alloc_w(size);

            if (nullptr == buf)
                DB::throwFromErrno(fmt::format("HugeAllocator: Cannot malloc {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        else
        {
            buf = hu_alloc_aligned(size, alignment);

            if (!buf)
                DB::throwFromErrno(fmt::format("Cannot allocate memory (posix_memalign) {}.", ReadableSize(size)),
                    DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, errno);

            if constexpr (clear_memory)
                memset(buf, 0, size);
        }

        return buf;
    }

    void freeNoTrack(void * buf)
    {
        hu_free_w(buf);
    }

    void checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to HugeAllocator. It indicates an error.", size);
    }
};

/** When using AllocatorWithStackMemory, located on the stack,
  *  GCC 4.9 mistakenly assumes that we can call `free` from a pointer to the stack.
  * In fact, the combination of conditions inside AllocatorWithStackMemory does not allow this.
  */
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

/// Prevent implicit template instantiation of HugeAllocator

extern template class HuAllocator<false>;
extern template class HuAllocator<true>;

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#endif
