#pragma once

#include <chrono>

#include <folly/fibers/TimedMutex.h>
#include <google/protobuf/io/coded_stream.h>
#include <tsl/sparse_map.h>
#include <Poco/AtomicCounter.h>

#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::HybridCache
{
// NVM index: map from key to value.
class Index
{
public:
    using SharedMutex = folly::fibers::TimedRWMutexWritePriority<folly::fibers::Baton>;

    static constexpr std::chrono::seconds kQuantileWindowSize{1};

    Index() = default;
    Index(const Index &) = delete;
    Index & operator=(const Index &) = delete;

    void persist(google::protobuf::io::CodedOutputStream * stream) const;
    void recover(google::protobuf::io::CodedInputStream * stream);

    struct PACKED_LINLINE ItemRecord
    {
        // encoded address
        UInt32 address{0};
        // a hint for the item size
        UInt16 size_hint{0};
        // total hits during this item's entire lifetime in cache
        UInt8 total_hits{0};
        // hits during the current window for this item
        UInt8 current_hits{0};

        explicit ItemRecord(UInt32 address_ = 0, UInt16 size_hint_ = 0, UInt8 total_hits_ = 0, UInt8 current_hits_ = 0)
            : address(address_), size_hint(size_hint_), total_hits(total_hits_), current_hits(current_hits_)
        {
        }
    };
    static_assert(8 == sizeof(ItemRecord), "ItemRecord size is 8 bytes");

    struct LookupResult
    {
        friend class Index;

        bool isFound() const { return found; }

        ItemRecord getRecord() const
        {
            chassert(found);
            return record;
        }

        UInt32 getAddress() const
        {
            chassert(found);
            return record.address;
        }

        UInt16 getSizeHint() const
        {
            chassert(found);
            return record.size_hint;
        }

        UInt8 getTotalHits() const
        {
            chassert(found);
            return record.total_hits;
        }

        UInt8 getCurrentHits() const
        {
            chassert(found);
            return record.current_hits;
        }

    private:
        ItemRecord record;
        bool found{false};
    };

    // Gets value and update tracking counters
    LookupResult lookup(UInt64 key);

    // Gets value without updating tracking counters
    LookupResult peek(UInt64 key) const;

    // Overwrites existing key if exists with new address adn size. If the entry was successfully overwritten,
    // LookupResult returns <true, OldRecord>.
    LookupResult insert(UInt64 key, UInt32 address, UInt16 size_hint);

    // Replaces old address with new address if there exists the key with the identical old address.
    bool replaceIfMatch(UInt64 key, UInt32 new_address, UInt32 old_address);

    // If the entry was successfully removed, LookupResult returns <true, DelRecord>.
    LookupResult remove(UInt64 key);

    // Removes only if both key and address match.
    bool removeIfMatch(UInt64 key, UInt32 address);

    // Update hits information of a key.
    void setHits(UInt64 key, UInt8 current_hits, UInt8 total_hits);

    // Resets all the buckets to the initial state.
    void reset();

    // Walks buckets and computes total index entry count
    size_t compuiteSize() const;

private:
    static constexpr UInt32 kNumBuckets{64 * 1024};
    static constexpr UInt32 kNumMutexes{1024};

    using Map = tsl::sparse_map<UInt32, ItemRecord>;

    static UInt32 bucket(UInt64 hash) { return (hash >> 32) & (kNumBuckets - 1); }

    static UInt32 subkey(UInt64 hash) { return hash & 0xffffffffu; }

    SharedMutex & getMutexOfBucket(UInt32 bucket) const
    {
        chassert(isPowerOf2(kNumMutexes));
        return mutex[bucket & (kNumMutexes - 1)];
    }

    SharedMutex & getMutex(UInt64 hash) const
    {
        auto b = bucket(hash);
        return getMutexOfBucket(b);
    }

    Map & getMap(UInt64 hash) const
    {
        auto b = bucket(hash);
        return buckets[b];
    }

    void trackRemove(UInt8 total_hits);

    mutable Poco::AtomicCounter unaccessed_items;

    std::unique_ptr<SharedMutex[]> mutex{new SharedMutex[kNumMutexes]};
    std::unique_ptr<Map[]> buckets{new Map[kNumBuckets]};
};
}
