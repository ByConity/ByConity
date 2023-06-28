//
// Created by 袁宇豪 on 9/18/22.
//
#pragma once

#ifndef CLICKHOUSE_SKETCHALLOCATOR_H
#define CLICKHOUSE_SKETCHALLOCATOR_H

#include <Common/Allocator.h>

template<typename T>
class SketchAllocator : public std::allocator<T>
{
public:
    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef std::size_t size_type;

public:
    TrackAllocator<value_type> allocator;

public:
    inline SketchAllocator() : allocator() {}
    inline ~SketchAllocator() {}
    inline SketchAllocator(SketchAllocator const& value)
        : std::allocator<T>(), allocator(value.allocator) {}

    // memory allocation
    inline pointer allocate(size_type cnt,
                            const void* constPointer = nullptr)
    {
        return allocator.allocate(cnt, constPointer);
    }
    inline void deallocate(pointer p, size_type size) {
        allocator.deallocate(p, size);
    }

    inline size_type max_size() const
    {
        return allocator.max_size();
    }

    inline void destroy(pointer p)
    {
        allocator.destroy(p);
    }

    inline bool operator==(SketchAllocator const& a)
    {
        return allocator.operator==(a.allocator);
    }

    inline bool operator!=(SketchAllocator const& a)
    {
        return allocator.operator!=(a.allocator);
    }
};

using AggregateFunctionHllSketchAllocator = SketchAllocator<uint8_t>;

using AggregateFunctionThetaSketchAllocator = SketchAllocator<uint64_t>;

using AggregateFunctionFISketchAllocator = SketchAllocator<uint8_t>;

#endif //CLICKHOUSE_SKETCHALLOCATOR_H
