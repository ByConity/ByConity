#pragma once

#include <random>

#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <common/StringRef.h>
#include <common/defines.h>

namespace DB::HybridCache
{
class PercentageReinsertionPolicy : public BlockCacheReinsertionPolicy
{
public:
    explicit PercentageReinsertionPolicy(UInt32 percentage_) : percentage{percentage_} { chassert(percentage_ > 0 && percentage_ <= 100); }

    bool shouldReinsert(StringRef /*key*/) override
    {
        return std::uniform_int_distribution<UInt32>(0, UINT32_MAX)(thread_local_rng) % 100 < percentage;
    }

private:
    const UInt32 percentage;
};
}
