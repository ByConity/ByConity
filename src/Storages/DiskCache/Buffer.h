#pragma once

#include <cstdlib>
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

    constexpr BufferViewT(size_t size, Type * data) : size_{size}, data_{data} { }

    BufferViewT(const BufferViewT &) = default;
    BufferViewT & operator=(const BufferViewT &) = default;

    // Return true if data is nullptr
    bool isNull() const { return data_ == nullptr; }

    // Return byte at specified index. This view must NOT be null. Caller is
    // responsible for ensuring the index is within bounds.
    UInt8 byteAt(size_t idx) const
    {
        chassert(data_);
        chassert(idx < size_);
        return data_[idx];
    }

    // Return beginning of the data
    Type * data() const { return data_; }

    // Return end of the data
    Type * dataEnd() const { return data_ + size_; }

    // Return size of the view in bytes
    size_t size() const { return size_; }

    void copyTo(void * dst) const
    {
        if (data_ != nullptr)
        {
            chassert(dst != nullptr);
            std::memcpy(dst, data_, size_);
        }
    }

    // Return a new view from part of this current view. This view must NOT be
    // null.
    BufferViewT slice(size_t offset, size_t size) const
    {
        // Use - instead of + to avoid potential overflow problem
        chassert(offset <= size_);
        chassert(size <= size_ - offset);
        return BufferViewT{size, data_ + offset};
    }

    // Two views are equal if their contents are identical
    bool operator==(BufferViewT other) const { return size_ == other.size_ && (size_ == 0 || std::memcmp(other.data_, data_, size_) == 0); }

    bool operator!=(BufferViewT other) const { return !(*this == other); }

private:
    size_t size_{};
    Type * data_{};
};

using BufferView = BufferViewT<const UInt8>;
using MutableBufferView = BufferViewT<UInt8>;

struct BufferDeleter
{
    void operator()(void * ptr) const { std::free(ptr); }
};

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
    explicit Buffer(size_t size) : size_{size}, data_{allocate(size)} { }

    // Create a new, empty, and aligned buffer
    Buffer(size_t size, size_t alignment) : size_{size}, data_{allocate(size, alignment)} { }

    Buffer(const Buffer &) = delete;
    Buffer & operator=(const Buffer &) = delete;

    Buffer(Buffer && other) noexcept = default;
    Buffer & operator=(Buffer &&) noexcept = default;

    // Return a read-only view
    BufferView view() const { return BufferView{size_, data()}; }

    // Return a mutable view
    MutableBufferView mutableView() { return MutableBufferView{size_, data()}; }

    // Return true if data is nullptr
    bool isNull() const { return data_ == nullptr; }

    // Return read-only start of the data
    const UInt8 * data() const { return data_.get() + data_start_offset_; }

    // Return mutable start of the data
    UInt8 * data() { return data_.get() + data_start_offset_; }

    // Return size in bytes for the data
    size_t size() const { return size_; }

    // Copy copies size_ number of bytes from dataOffsetStart_ to a new buffer
    // and returns the new buffer
    Buffer copy(size_t alignment = 0) const
    {
        return (alignment == 0) ? copyInternal(Buffer{size_}) : copyInternal(Buffer{size_, alignment});
    }

    // This buffer must NOT be null and it must have sufficient capacity
    // to copy the data from the source view.
    void copyFrom(size_t offset, BufferView view)
    {
        chassert(offset + view.size() <= size_);
        chassert(data_ != nullptr);
        if (view.data() != nullptr)
        {
            std::memcpy(data() + offset, view.data(), view.size());
        }
    }

    // Adjust the data start offset forwards to include less valid data
    // This moves the data pointer forwards so that the first amount bytes are no
    // longer considered valid data.  The caller is responsible for ensuring that
    // amount is less than or equal to the actual data length.
    //
    // This does not modify any actual data in the buffer.
    void trimStart(size_t amount)
    {
        chassert(amount <= size_);
        data_start_offset_ += amount;
        size_ -= amount;
    }

    // Shrink buffer logical size (doesn't reallocate)
    void shrink(size_t size)
    {
        chassert(size <= size_);
        size_ = size;
    }

    // Clear the buffer
    void reset()
    {
        size_ = 0;
        data_.reset();
    }

private:
    Buffer copyInternal(Buffer buf) const
    {
        if (data_)
        {
            chassert(buf.data_ != nullptr);
            std::memcpy(buf.data(), data(), size_);
        }
        return buf;
    }

    static UInt8 * allocate(size_t size)
    {
        auto * ptr = reinterpret_cast<UInt8 *>(std::malloc(size));
        if (!ptr)
        {
            throw std::bad_alloc();
        }
        return ptr;
    }

    static UInt8 * allocate(size_t size, size_t alignment)
    {
        chassert(isPowerOf2(alignment));
        chassert(size % alignment == 0u);
        auto * ptr = reinterpret_cast<UInt8 *>(::aligned_alloc(alignment, size));
        if (!ptr)
        {
            throw std::bad_alloc();
        }
        return ptr;
    }

    // size_ represents the size of valid data in the data_, i.e., "size_" number
    // of bytes from startOffset in data_ are considered valid in the Buffer
    size_t size_{};

    // data_start_offset_ is the offset in data_ where the actual(user-interested)
    // data starts. This helps in skipping past unnecessary data in the buffer
    // without having to copy it. There could be unnecessary data in the buffer
    // due to read/write from/to a block-aligned address when the actual data
    // starts somewhere in the middle(ie not at the block aligned address).
    size_t data_start_offset_{0};
    std::unique_ptr<UInt8[], BufferDeleter> data_{};
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
