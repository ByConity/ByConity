#pragma once

#include <Storages/DiskCache/Buffer.h>

namespace DB::HybridCache
{
// Simple FIFO allocator that once full the only
// way to free up more space is by removing entries at the
// front.
class __attribute__((__packed__)) BucketStorage
{
public:
    // Allocation is that which returned to user when they
    // allocate from the BucketStorage.
    class Allocation
    {
    public:
        // indicate if the end of storage is reached.
        bool done() const { return view_.isNull(); }

        // return a mutable view where caller can read or modify data
        MutableBufferView view() const { return view_; }

        // return the index of this allocation in the BucketStorage
        UInt32 position() const { return position_; }

    private:
        friend BucketStorage;

        Allocation() = default;
        Allocation(MutableBufferView v, UInt32 p) : view_{v}, position_{p} { }

        MutableBufferView view_{};
        UInt32 position_{};
    };

    static UInt32 slotSize(UInt32 size) { return kAllocationOverhead + size; }

    // construct a BucketStorage with given capacity, a placement new is required.
    explicit BucketStorage(UInt32 capacity) : capacity_{capacity} { }

    // allocate a space under this bucket storage
    Allocation allocate(UInt32 size);

    UInt32 capacity() const { return capacity_; }

    UInt32 remainingCapacity() const { return capacity_ - end_offset_; }

    UInt32 numAllocations() const { return num_allocations_; }

    // remove the given allocation in the bucket storage.
    void remove(Allocation alloc);

    // remove the given list allocation in the bucket storage.
    void remove(const std::vector<Allocation> & allocs);

    // Removes every single allocation from the beginning, including this one.
    void removeUntil(Allocation alloc);

    // iterate the storage using Allocation
    Allocation getFirst() const;
    Allocation getNext(Allocation alloc) const;

    // offset of the Allocation within the Bucket
    UInt32 getOffset(Allocation & alloc) { return alloc.view().data() - data_; }

private:
    // Slot represents a physical slot in the storage. User does not use
    // this directly but instead uses Allocation.
    struct __attribute__((__packed__)) Slot
    {
        UInt32 size{};
        UInt8 data[];
        explicit Slot(UInt32 s) : size{s} { }
    };

    bool canAllocate(UInt32 size) const { return static_cast<uint64_t>(end_offset_) + slotSize(size) <= capacity_; }

    static const UInt32 kAllocationOverhead;

    const UInt32 capacity_{};
    UInt32 num_allocations_{};
    UInt32 end_offset_{};
    mutable UInt8 data_[];
};
}
