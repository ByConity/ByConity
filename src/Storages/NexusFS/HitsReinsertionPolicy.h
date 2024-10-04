#pragma once

#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/NexusFS/NexusFSIndex.h>
#include <common/StringRef.h>

namespace DB::NexusFSComponents
{
class NexusFSHitsReinsertionPolicy : public HybridCache::BlockCacheReinsertionPolicy
{
public:
    explicit NexusFSHitsReinsertionPolicy(UInt8 hits_threshold_, const NexusFSIndex & index_) : hits_threshold{hits_threshold_}, index{index_} { }

    bool shouldReinsert(StringRef key) override
    {
        const auto lr = index.peek(makeHashKey(HybridCache::BufferView{key.size, reinterpret_cast<const UInt8 *>(key.data)}).keyHash());
        return lr.isFound() && lr.getCurrentHits() >= hits_threshold;
    }

private:
    const UInt8 hits_threshold{};

    const NexusFSIndex & index;
};
}
