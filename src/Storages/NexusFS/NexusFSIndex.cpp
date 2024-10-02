#include <Storages/NexusFS/NexusFSIndex.h>

#include <limits>

#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Common/Exception.h>
#include "Storages/DiskCache/Types.h"

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace DB::NexusFSComponents
{
namespace
{
    UInt8 safeInc(UInt8 val)
    {
        if (val < std::numeric_limits<UInt8>::max())
            return val + 1;
        return val;
    }
}

void NexusFSIndex::setHits(UInt64 key, UInt8 current_hits, UInt8 total_hits)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        it.value().current_hits = current_hits;
        it.value().total_hits = total_hits;
    }
}

NexusFSIndex::LookupResult NexusFSIndex::lookup(UInt64 key)
{
    LookupResult result;
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        result.found = true;
        result.record = it->second;
        it.value().total_hits = safeInc(result.record.total_hits);
        it.value().current_hits = safeInc(result.record.current_hits);
    }
    return result;
}

NexusFSIndex::LookupResult NexusFSIndex::peek(UInt64 key) const
{
    LookupResult result;
    const auto & map = getMap(key);
    auto lock = std::shared_lock{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        result.found = true;
        result.record = it->second;
    }
    return result;
}

NexusFSIndex::LookupResult NexusFSIndex::insert(UInt64 key, RelAddress address, UInt32 size)
{
    LookupResult result;
    auto & map = getMap(key);
    // auto handle = std::make_shared<SegmentHandle>();
    // handle->loadedToDisk(address);

    auto guard = std::lock_guard{getMutex(key)};
    auto ret = map.try_emplace(subkey(key), address, size);
    chassert(ret.second);
    result.found = true;
    result.record = ret.first->second;

    return result;
}

// bool NexusFSIndex::replaceIfMatch(UInt64 key, RelAddress new_address, RelAddress old_address)
// {
//     auto & map = getMap(key);
//     auto guard = std::lock_guard{getMutex(key)};

//     auto it = map.find(subkey(key));
//     if (it != map.end() && it->second.address == old_address)
//     {
//         it.value().address = new_address;
//         it.value().current_hits = 0;
//         return true;
//     }
//     return false;
// }

void NexusFSIndex::trackRemove(UInt8 total_hits)
{
    if (total_hits == 0)
        unaccessed_items++;
}

NexusFSIndex::LookupResult NexusFSIndex::remove(UInt64 key)
{
    LookupResult result;
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        result.found = true;
        result.record = it->second;

        trackRemove(it->second.total_hits);
        map.erase(it);
    }
    return result;
}

bool NexusFSIndex::removeIfMatch(UInt64 key, RelAddress address)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end() && it->second.address == address)
    {
        trackRemove(it->second.total_hits);
        map.erase(it);
        return true;
    }
    return false;
}

void NexusFSIndex::reset()
{
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        auto guard = std::lock_guard{getMutexOfBucket(i)};
        buckets[i].clear();
    }
    unaccessed_items = 0;
}

size_t NexusFSIndex::compuiteSize() const
{
    size_t size = 0;
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        auto guard = std::lock_guard{getMutexOfBucket(i)};
        size += buckets[i].size();
    }
    return size;
}

void NexusFSIndex::persist(google::protobuf::io::CodedOutputStream * stream) const
{
    Protos::NexusFSIndexBucket bucket;
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        bucket.set_bucket_id(i);
        for (const auto & [key, v] : buckets[i])
        {
            auto * entry = bucket.add_entries();
            entry->set_key(key);
            entry->set_address_rid(v.address.rid().index());
            entry->set_address_offset(v.address.offset());
            entry->set_size(v.size);
            entry->set_total_hits(v.total_hits);
            entry->set_current_hits(v.current_hits);
        }

        google::protobuf::util::SerializeDelimitedToCodedStream(bucket, stream);
        bucket.clear_entries();
    }
}

void NexusFSIndex::recover(google::protobuf::io::CodedInputStream * stream)
{
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        Protos::NexusFSIndexBucket bucket;
        google::protobuf::util::ParseDelimitedFromCodedStream(&bucket, stream, nullptr);
        UInt32 id = bucket.bucket_id();
        if (id >= kNumBuckets)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid bucket id. Max buckets: {}, bucket id: {}", kNumBuckets, id);

        for (const auto & entry : bucket.entries())
        {
            buckets[id].try_emplace(
                entry.key(),
                RelAddress(HybridCache::RegionId(entry.address_rid()), entry.address_offset()),
                static_cast<UInt32>(entry.size()),
                static_cast<UInt8>(entry.total_hits()),
                static_cast<UInt8>(entry.current_hits()));
        }
    }
}

}
