#pragma once

#include <cstddef>
#include <parallel_hashmap/phmap.h>
#include <Common/config.h>
#include <Common/Allocator.h>
#include <Common/HuAllocator.h>


namespace DB
{
template <typename T>
#if USE_HUALLOC
class StdNoTrackAllocator : private HuAllocator<true>
#else
class StdNoTrackAllocator : private Allocator<true, false>
#endif
{
public:
    using value_type = T;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using reference = value_type &;
    using const_reference = const value_type &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
	using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
	using propagate_on_container_swap = std::true_type; // to avoid the undefined behavior
    using is_always_equal = std::true_type;
    StdNoTrackAllocator() = default;

    template <typename TT>
    explicit StdNoTrackAllocator(const StdNoTrackAllocator<TT>&) {}


    // convert an StdNoTrackAllocator<T> to StdNoTrackAllocator<U>
    template<typename U>
    struct Rebind
    {
        using other = StdNoTrackAllocator<U>;
    };

    // inline ~StdNoTrackAllocator() = default;
    // inline StdNoTrackAllocator(StdNoTrackAllocator const&) = default;

    // address
    inline pointer address(reference r) { return &r;  }
    inline const_pointer address(const_reference r) { return &r;  }

    // memory allocation/deallocate
    inline pointer allocate(size_type n)
    {
        #if USE_HUALLOC
        return static_cast<T*>(HuAllocator::allocNoTrack(n * sizeof(T), 0));
        #else
        return static_cast<T*>(Allocator::allocNoTrack(n * sizeof(T), 0));
        #endif
    }

    inline void deallocate(pointer p, size_type /*n*/)
    {
        #if USE_HUALLOC
        HuAllocator::freeNoTrack(p);
        #else
        Allocator::freeNoTrack(p);
        #endif
    }

    // size
    inline size_type maxSize() const
    {
        return std::numeric_limits<size_type>::max() / sizeof(T);
    }

    // construction/destruction
    inline void construct(pointer p, const T& t) { new(p) T(t);  }
    inline void destroy(pointer p) { p->~T();  }

    inline bool operator==(StdNoTrackAllocator const&) { return true;  }
    inline bool operator!=(StdNoTrackAllocator const& a) { return !operator==(a);  }

#ifndef NDEBUG
    /// In debug builds, request mmap() at random addresses (a kind of ASLR), to
    /// reproduce more memory stomping bugs. Note that Linux doesn't do it by
    /// default. This may lead to worse TLB performance.
    void * getMmapHint()
    {
#if !defined(__APPLE__)
        //         return reinterpret_cast<void *>(std::uniform_int_distribution<intptr_t>(0x100000000000UL, 0x700000000000UL)(thread_local_rng));
        // #else
        return nullptr;
#endif
    }
#else
    void * getMmapHint()
    {
        return nullptr;
    }
#endif


    friend bool operator==(const StdNoTrackAllocator&, const StdNoTrackAllocator&) { return true; }
};
}
