#include <Storages/DiskCache/BucketStorage.h>

namespace DB::HybridCache
{
static_assert(sizeof(BucketStorage) == 12, "BucketStorage overhead");

const UInt32 BucketStorage::kAllocationOverhead = sizeof(BucketStorage::Slot);

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
BucketStorage::Allocation BucketStorage::allocate(UInt32 size)
{
    if (!canAllocate(size))
        return {};

    auto * slot = new (data_ + end_offset_) Slot(size);
    end_offset_ += slotSize(size);
    num_allocations_++;
    return {MutableBufferView{slot->size, slot->data}, num_allocations_ - 1};
}

void BucketStorage::remove(Allocation alloc)
{
    remove(std::vector<Allocation>({alloc}));
}

void BucketStorage::remove(const std::vector<Allocation> & allocs)
{
    // Remove triggers a compaction.
    //
    //                         tail
    //  |---|REMOVED|-----|REMOVED|-----|~~~~|
    //
    // after compaction
    //                  tail
    //  |---------------|~~~~~~~~~~~|
    if (allocs.empty())
        return;

    UInt32 src_offset = 0;
    UInt32 dst_offset = 0;
    for (const auto & alloc : allocs)
    {
        UInt32 alloc_offset = alloc.view().data() - data_;
        UInt32 removed_offset = alloc_offset - kAllocationOverhead;
        // We have valid data from [srcOffset, removedOffset)
        if (src_offset != removed_offset)
        {
            UInt32 len = removed_offset - src_offset;
            if (dst_offset != src_offset)
                std::memmove(data_ + dst_offset, data_ + src_offset, len);
            dst_offset += len;
        }
        // update the offset which (could) contain next valid data
        src_offset = alloc_offset + alloc.view().size();
        num_allocations_--;
    }

    // copy the rest of data after the last removed alloc if any
    if (src_offset != end_offset_)
    {
        UInt32 len = end_offset_ - src_offset;
        std::memmove(data_ + dst_offset, data_ + src_offset, len);
        dst_offset += len;
    }
    // update end offset to point the right next byte of the data copied
    end_offset_ = dst_offset;
}

void BucketStorage::removeUntil(Allocation alloc)
{
    // Remove everything until (and include) "alloc"
    //
    //                         tail
    //  |----------------|-----|~~~~|
    //  ^                ^
    //  begin            offset
    //  remove this whole range
    //
    //        tail
    //  |-----|~~~~~~~~~~~~~~~~~~~~~|
    if (alloc.done())
        return;

    UInt32 offset = alloc.view().data() + alloc.view().size() - data_;
    if (offset > end_offset_)
        return;

    std::memmove(data_, data_ + offset, end_offset_ - offset);
    end_offset_ -= offset;
    num_allocations_ -= alloc.position() + 1;
}

BucketStorage::Allocation BucketStorage::getFirst() const
{
    if (end_offset_ == 0)
        return {};
    auto * slot = reinterpret_cast<Slot *>(data_);
    return {MutableBufferView{slot->size, slot->data}, 0};
}

BucketStorage::Allocation BucketStorage::getNext(BucketStorage::Allocation alloc) const
{
    if (alloc.done())
        return {};

    auto * next = reinterpret_cast<Slot *>(alloc.view().data() + alloc.view().size());
    if (reinterpret_cast<UInt8 *>(next) - data_ >= end_offset_)
        return {};
    return {MutableBufferView{next->size, next->data}, alloc.position() + 1};
}
}
