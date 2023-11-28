#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>

#include <IO/BufferWithOwnMemory.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/types.h>


namespace DB::HybridCache
{
// View into a buffer. Doesn't own data. Caller must ensure buffer
// lifetime. We offer two versions:
//  1. read-only - BufferView
//  2. mutable - MutableBufferView
template <typename T>
class BufferViewT
{
public:
    using Type = T;

    constexpr BufferViewT() : BufferViewT{0, nullptr} { }

    constexpr BufferViewT(size_t size, Type * data) : data_size{size}, data_ptr{data} { }

    BufferViewT(const BufferViewT &) = default;
    BufferViewT & operator=(const BufferViewT &) = default;

    // Return true if data is nullptr
    bool isNull() const { return data_ptr == nullptr; }

    // Return byte at specified index. This view must NOT be null. Caller is
    // responsible for ensuring the index is within bounds.
    UInt8 byteAt(size_t idx) const
    {
        chassert(data_ptr);
        chassert(idx < data_size);
        return data_ptr[idx];
    }

    // Return beginning of the data
    Type * data() const { return data_ptr; }

    // Return end of the data
    Type * dataEnd() const { return data_ptr + data_size; }

    // Return size of the view in bytes
    size_t size() const { return data_size; }

    void copyTo(void * dst) const
    {
        if (data_ptr != nullptr)
        {
            chassert(dst != nullptr);
            std::memcpy(dst, data_ptr, data_size);
        }
    }

    // Return a new view from part of this current view. This view must NOT be
    // null.
    BufferViewT slice(size_t offset, size_t size) const
    {
        // Use - instead of + to avoid potential overflow problem
        chassert(offset <= data_size);
        chassert(size <= data_size - offset);
        return BufferViewT{size, data_ptr + offset};
    }

    // Two views are equal if their contents are identical
    bool operator==(BufferViewT other) const
    {
        return data_size == other.data_size && (data_size == 0 || std::memcmp(other.data_ptr, data_ptr, data_size) == 0);
    }

    bool operator!=(BufferViewT other) const { return !(*this == other); }

private:
    size_t data_size{};
    Type * data_ptr{};
};

using BufferView = BufferViewT<const UInt8>;
using MutableBufferView = BufferViewT<UInt8>;

// Byte buffer. Manages buffer lifetime.
class Buffer
{
public:
    Buffer() = default;

    // Copy data from a view
    explicit Buffer(BufferView view) : Buffer{view.size()} { view.copyTo(data()); }

    // Copy data from a view into an aligned buffer
    Buffer(BufferView view, size_t alignment) : Buffer{view.size(), alignment} { view.copyTo(data()); }

    // Create a new, empty buffer
    explicit Buffer(size_t size) : data_size{size}, memory{size} { }

    // Create a new, empty, and aligned buffer
    Buffer(size_t size, size_t alignment) : data_size{size}, memory{size, alignment} { }

    Buffer(const Buffer &) = delete;
    Buffer & operator=(const Buffer &) = delete;

    Buffer(Buffer && other) noexcept = default;
    Buffer & operator=(Buffer &&) noexcept = default;

    // Return a read-only view
    BufferView view() const { return BufferView{data_size, data()}; }

    // Return a mutable view
    MutableBufferView mutableView() { return MutableBufferView{data_size, data()}; }

    // Return true if data is nullptr
    bool isNull() const { return memory.data() == nullptr; }

    // Return read-only start of the data
    const UInt8 * data() const { return reinterpret_cast<const UInt8 *>(memory.data()) + data_start_offset; }

    // Return mutable start of the data
    UInt8 * data() { return reinterpret_cast<UInt8 *>(memory.data()) + data_start_offset; }
    // Return size in bytes for the data
    size_t size() const { return data_size; }

    // Copy copies size_ number of bytes from dataOffsetStart_ to a new buffer
    // and returns the new buffer
    Buffer copy(size_t alignment = 0) const
    {
        return (alignment == 0) ? copyInternal(Buffer{data_size}) : copyInternal(Buffer{data_size, alignment});
    }

    // This buffer must NOT be null and it must have sufficient capacity
    // to copy the data from the source view.
    void copyFrom(size_t offset, BufferView view)
    {
        chassert(offset + view.size() <= data_size);
        chassert(!isNull());
        if (!view.isNull())
            std::memcpy(data() + offset, view.data(), view.size());
    }

    // Adjust the data start offset forwards to include less valid data
    // This moves the data pointer forwards so that the first amount bytes are no
    // longer considered valid data.  The caller is responsible for ensuring that
    // amount is less than or equal to the actual data length.
    //
    // This does not modify any actual data in the buffer.
    void trimStart(size_t amount)
    {
        chassert(amount <= data_size);
        data_start_offset += amount;
        data_size -= amount;
    }

    // Shrink buffer logical size (doesn't reallocate)
    void shrink(size_t size)
    {
        chassert(size <= data_size);
        data_size = size;
    }

    // Clear the buffer
    void reset()
    {
        data_size = 0;
        memory.freeResource();
    }

    Memory<>& getMemory() { return memory; }

private:
    Buffer copyInternal(Buffer buf) const
    {
        if (!isNull())
        {
            chassert(!buf.isNull());
            std::memcpy(buf.data(), data(), data_size);
        }
        return buf;
    }

    // size_ represents the size of valid data in the data_, i.e., "size_" number
    // of bytes from startOffset in data_ are considered valid in the Buffer
    size_t data_size{};

    // data_start_offset_ is the offset in data_ where the actual(user-interested)
    // data starts. This helps in skipping past unnecessary data in the buffer
    // without having to copy it. There could be unnecessary data in the buffer
    // due to read/write from/to a block-aligned address when the actual data
    // starts somewhere in the middle(ie not at the block aligned address).
    size_t data_start_offset{0};
    Memory<> memory;
};

inline BufferView toView(MutableBufferView mutable_view)
{
    return {mutable_view.size(), mutable_view.data()};
}

// Trailing 0 is not included
inline BufferView makeView(const char * cstr)
{
    return {std::strlen(cstr), reinterpret_cast<const UInt8 *>(cstr)};
}

inline BufferView makeView(StringRef str)
{
    return {str.size, reinterpret_cast<const UInt8 *>(str.data)};
}

inline StringRef toStringRef(BufferView view)
{
    return {reinterpret_cast<const char *>(view.data()), view.size()};
}
}
