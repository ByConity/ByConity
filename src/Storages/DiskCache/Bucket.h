#pragma once

#include <Storages/DiskCache/BucketStorage.h>
#include <Storages/DiskCache/Types.h>

namespace DB::HybridCache
{
// A bucket is the fundamental unit of read and write onto the device.
// Read Path: 1. read an entire buffer from device 2. search for the key
// Write Path: 1. Insert the new entry in memory 2. write back to device
class PACKED_LINLINE Bucket
{
public:
    // Iterator to bucket's items.
    class Iterator
    {
    public:
        // return whether the iteration has reached the end
        bool done() const { return iterator.done(); }

        BufferView key() const;
        UInt64 keyHash() const;
        BufferView value() const;

        bool keyEqualsTo(HashedKey key) const;

    private:
        friend Bucket;

        explicit Iterator(BucketStorage::Allocation itr) : iterator{itr} { }

        BucketStorage::Allocation iterator;
    };

    // User will pass in a view that contains the memory that is a Bucket
    static UInt32 computeChecksum(BufferView view);

    // Initialize a brand new Bucket given a piece of memory in the case
    // that the existing bucket is invalid. (I.e. checksum or generation
    // and generation time for.
    static Bucket & initNew(MutableBufferView view, UInt64 generation_time);

    UInt32 getChecksum() const { return checksum; }

    void setChecksum(UInt32 checksum_) { checksum = checksum_; }

    // return the generation time of the bucket, if this is mismatch with
    // the one in BigHash data in the bucket is invalid.
    UInt64 generationTime() const { return generation_time; }

    UInt32 size() const { return storage.numAllocations(); }

    UInt32 remainingBytes() const { return storage.remainingCapacity(); }

    // Look up for the value corresponding to a key.
    // BufferView::isNull() == true if not found.
    BufferView find(HashedKey key) const;

    // Note: this does *not* replace an existing key! User must make sure to
    //       remove an existing key before calling insert.
    //
    // Insert into the bucket. Trigger eviction and invoke @destructorCb if
    // not enough space.
    // Return <number of entries evicted, number of entries expired> pair
    std::pair<UInt32, UInt32>
    insert(HashedKey key, BufferView value, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    // Remove an entry corresponding to the key. If found, invoke @destructorCb
    // before returning true. Return number of entries removed.
    UInt32 remove(HashedKey key, const DestructorCallback & destructor_callback);

    // return a BufferView for the item randomly sampled in the Bucket
    std::pair<std::string, BufferView> getRandomAlloc();

    // return an iterator of items in the bucket
    Iterator getFirst() const;
    Iterator getNext(Iterator itr) const;

private:
    Bucket(UInt64 generation_time_, UInt32 capacity_) : generation_time{generation_time_}, storage{capacity_} { }

    // Reserve enough space for @size by evicting. Return number of evictions.
    // Returns <number of evictions, number of expirations> pair
    std::pair<UInt32, UInt32> makeSpace(UInt32 size, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    UInt32 removeExpired(BucketStorage::Allocation itr, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback);

    UInt32 checksum{};
    UInt64 generation_time{};
    BucketStorage storage;
};


// This maps to exactly how an entry is stored in a bucket on device.
class PACKED_LINLINE BucketEntry
{
public:
    static UInt32 computeSize(UInt32 key_size, UInt32 value_size) { return sizeof(BucketEntry) + key_size + value_size; }

    // construct the BucketEntry with given memory using placement new
    static BucketEntry & create(MutableBufferView storage, HashedKey key, BufferView value)
    {
        new (storage.data()) BucketEntry{key, value};
        return reinterpret_cast<BucketEntry &>(*storage.data());
    }

    BufferView key() const { return {key_size, data}; }

    HashedKey hashedKey() const { return HashedKey::precomputed(toStringRef(key()), hash_key); }

    bool keyEqualsTo(HashedKey key) const { return key == hashedKey(); }

    UInt64 keyHash() const { return hash_key; }

    BufferView value() const { return {value_size, data + key_size}; }

private:
    BucketEntry(HashedKey key, BufferView value)
        : key_size{static_cast<UInt32>(key.key().size)}, value_size{static_cast<UInt32>(value.size())}, hash_key{key.keyHash()}
    {
        static_assert(sizeof(BucketEntry) == 16, "BucketEntry overhead");
        makeView(key.key()).copyTo(data);
        value.copyTo(data + key_size);
    }

    const UInt32 key_size{};
    const UInt32 value_size{};
    const UInt64 hash_key{};
    UInt8 data[];
};
}
