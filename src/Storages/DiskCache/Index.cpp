#include <Storages/DiskCache/Index.h>

#include <limits>
#include <mutex>
#include <shared_mutex>

namespace DB::HybridCache
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

void Index::setHits(UInt64 key, UInt8 current_hits, UInt8 total_hits)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        it->second.current_hits = current_hits;
        it->second.total_hits = total_hits;
    }
}

Index::LookupResult Index::lookup(UInt64 key)
{
    LookupResult result;
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end())
    {
        result.found = true;
        result.record = it->second;
        it->second.total_hits = safeInc(result.record.total_hits);
        it->second.current_hits = safeInc(result.record.current_hits);
    }
    return result;
}

Index::LookupResult Index::peek(UInt64 key) const
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

Index::LookupResult Index::insert(UInt64 key, UInt32 address, UInt16 size_hint)
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
        it->second.address = address;
        it->second.current_hits = 0;
        it->second.total_hits = 0;
        it->second.size_hint = size_hint;
    }
    else
        map[key] = ItemRecord{address, size_hint};
    return result;
}

bool Index::replaceIfMatch(UInt64 key, UInt32 new_address, UInt32 old_address)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end() && it->second.address == old_address)
    {
        it->second.address = new_address;
        it->second.current_hits = 0;
        return true;
    }
    return false;
}

void Index::trackRemove(UInt8 total_hits)
{
    if (total_hits == 0)
        unaccessed_items++;
}

Index::LookupResult Index::remove(UInt64 key)
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
        map.set_deleted_key(it->first);
        map.erase(it);
    }
    return result;
}

bool Index::removeIfMatch(UInt64 key, UInt32 address)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end() && it->second.address == address)
    {
        trackRemove(it->second.total_hits);
        map.set_deleted_key(it->first);
        map.erase(it);
        return true;
    }
    return false;
}

void Index::reset()
{
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        auto guard = std::lock_guard{getMutexOfBucket(i)};
        buckets[i].clear();
    }
    unaccessed_items = 0;
}

size_t Index::compuiteSize() const
{
    size_t size = 0;
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        auto guard = std::lock_guard{getMutexOfBucket(i)};
        size += buckets[i].size();
    }
    return size;
}

void Index::persist(google::protobuf::io::CodedOutputStream * /*stream*/) const
{
}

void Index::recover(google::protobuf::io::CodedInputStream * /*stream*/)
{
}

}
