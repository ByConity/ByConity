#include <Storages/DiskCache/Index.h>

#include <limits>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

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
        it.value().current_hits = current_hits;
        it.value().total_hits = total_hits;
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
        it.value().total_hits = safeInc(result.record.total_hits);
        it.value().current_hits = safeInc(result.record.current_hits);
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
        it.value().address = address;
        it.value().current_hits = 0;
        it.value().total_hits = 0;
        it.value().size_hint = size_hint;
    }
    else
        map.try_emplace(subkey(key), address, size_hint);
    return result;
}

bool Index::replaceIfMatch(UInt64 key, UInt32 new_address, UInt32 old_address)
{
    auto & map = getMap(key);
    auto guard = std::lock_guard{getMutex(key)};

    auto it = map.find(subkey(key));
    if (it != map.end() && it->second.address == old_address)
    {
        it.value().address = new_address;
        it.value().current_hits = 0;
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

void Index::persist(google::protobuf::io::CodedOutputStream * stream) const
{
    Protos::IndexBucket bucket;
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        bucket.set_bucket_id(i);
        for (const auto & [key, v] : buckets[i])
        {
            auto * entry = bucket.add_entries();
            entry->set_key(key);
            entry->set_address(v.address);
            entry->set_size_hint(v.size_hint);
            entry->set_total_hits(v.total_hits);
            entry->set_current_hits(v.current_hits);
        }

        google::protobuf::util::SerializeDelimitedToCodedStream(bucket, stream);
        bucket.clear_entries();
    }
}

void Index::recover(google::protobuf::io::CodedInputStream * stream)
{
    for (UInt32 i = 0; i < kNumBuckets; i++)
    {
        Protos::IndexBucket bucket;
        google::protobuf::util::ParseDelimitedFromCodedStream(&bucket, stream, nullptr);
        UInt32 id = bucket.bucket_id();
        if (id >= kNumBuckets)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid bucket id. Max buckets: {}, bucket id: {}", kNumBuckets, id);

        for (const auto & entry : bucket.entries())
        {
            buckets[id].try_emplace(
                entry.key(),
                entry.address(),
                static_cast<UInt16>(entry.size_hint()),
                static_cast<UInt8>(entry.total_hits()),
                static_cast<UInt8>(entry.current_hits()));
        }
    }
}

}
