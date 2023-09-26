#pragma once

#include <Storages/DiskCache/BucketStorage.h>
#include <Storages/DiskCache/Types.h>

namespace DB::HybridCache
{
// A bucket is the fundamental unit of read and write onto the device.
// Read Path: 1. read an entire buffer from device 2. search for the key
// Write Path: 1. Insert the new entry in memory 2. write back to device
class __attribute__((__packed__)) Bucket
{
public:
    // Iterator to bucket's items.
    class Iterator
    {
    public:
        // return whether the iteration has reached the end
        bool done() const { return itr_.done(); }

        BufferView key() const;
        UInt64 keyHash() const;
        BufferView value() const;

        bool keyEqualsTo(HashedKey hk) const;

    private:
        friend Bucket;

        Iterator() = default;
        explicit Iterator(BucketStorage::Allocation itr) : itr_{itr} { }

        BucketStorage::Allocation itr_;
    };

    // User will pass in a view that contains the memory that is a Bucket
    static UInt32 computeChecksum(BufferView view);

    // Initialize a brand new Bucket given a piece of memory in the case
    // that the existing bucket is invalid. (I.e. checksum or generation
    // and generation time for.
    static Bucket & initNew(MutableBufferView view, UInt64 generation_time);

    UInt32 getChecksum() const { return checksum_; }

    void setChecksum(UInt32 checksum) { checksum_ = checksum; }

    // return the generation time of the bucket, if this is mismatch with
    // the one in BigHash data in the bucket is invalid.
    UInt64 generationTime() const { return generation_time_; }

    UInt32 size() const { return storage_.numAllocations(); }

    UInt32 remainingBytes() const { return storage_.remainingCapacity(); }

    // Look up for the value corresponding to a key.
    // BufferView::isNull() == true if not found.
    BufferView find(HashedKey hk) const;

    // Note: this does *not* replace an existing key! User must make sure to
    //       remove an existing key before calling insert.
    //
    // Insert into the bucket. Trigger eviction and invoke @destructorCb if
    // not enough space.
    // Return <number of entries evicted, number of entries expired> pair
    std::pair<UInt32, UInt32>
    insert(HashedKey hk, BufferView value, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    // Remove an entry corresponding to the key. If found, invoke @destructorCb
    // before returning true. Return number of entries removed.
    UInt32 remove(HashedKey hk, const DestructorCallback & destructor_callback);

    // return a BufferView for the item randomly sampled in the Bucket
    std::pair<std::string, BufferView> getRandomAlloc();

    // return an iterator of items in the bucket
    Iterator getFirst() const;
    Iterator getNext(Iterator itr) const;

private:
    Bucket(UInt64 generation_time, UInt32 capacity) : generation_time_{generation_time}, storage_{capacity} { }

    // Reserve enough space for @size by evicting. Return number of evictions.
    // Returns <number of evictions, number of expirations> pair
    std::pair<UInt32, UInt32> makeSpace(UInt32 size, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    UInt32 removeExpired(BucketStorage::Allocation itr, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    UInt32 checksum_{};
    UInt64 generation_time_{};
    BucketStorage storage_;
};


// This maps to exactly how an entry is stored in a bucket on device.
class __attribute__((__packed__)) BucketEntry
{
public:
    static UInt32 computeSize(UInt32 key_size, UInt32 value_size) { return sizeof(BucketEntry) + key_size + value_size; }

    // construct the BucketEntry with given memory using placement new
    static BucketEntry & create(MutableBufferView storage, HashedKey hk, BufferView value)
    {
        new (storage.data()) BucketEntry{hk, value};
        return reinterpret_cast<BucketEntry &>(*storage.data());
    }

    BufferView key() const { return {key_size_, data_}; }

    HashedKey hashedKey() const { return HashedKey::precomputed(toStringRef(key()), key_hash_); }

    bool keyEqualsTo(HashedKey hk) const { return hk == hashedKey(); }

    UInt64 keyHash() const { return key_hash_; }

    BufferView value() const { return {value_size_, data_ + key_size_}; }

private:
    BucketEntry(HashedKey hk, BufferView value)
        : key_size_{static_cast<UInt32>(hk.key().size)}, value_size_{static_cast<UInt32>(value.size())}, key_hash_{hk.keyHash()}
    {
        static_assert(sizeof(BucketEntry) == 16, "BucketEntry overhead");
        makeView(hk.key()).copyTo(data_);
        value.copyTo(data_ + key_size_);
    }

    const UInt32 key_size_{};
    const UInt32 value_size_{};
    const UInt64 key_hash_{};
    UInt8 data_[];
};
}
