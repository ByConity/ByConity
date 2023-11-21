#pragma once

#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Index.h>
#include <common/StringRef.h>

namespace DB::HybridCache
{
class HitsReinsertionPolicy : public BlockCacheReinsertionPolicy
{
public:
    explicit HitsReinsertionPolicy(UInt8 hits_threshold_, const Index & index_) : hits_threshold{hits_threshold_}, index{index_} { }

    bool shouldReinsert(StringRef key) override
    {
        const auto lr = index.peek(makeHashKey(BufferView{key.size, reinterpret_cast<const UInt8 *>(key.data)}).keyHash());
        return lr.isFound() && lr.getCurrentHits() >= hits_threshold;
    }

private:
    const UInt8 hits_threshold{};

    const Index & index;
};
}
