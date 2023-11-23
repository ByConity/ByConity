#pragma once

#include <common/StringRef.h>

namespace DB::HybridCache
{
class BlockCacheReinsertionPolicy
{
public:
    virtual ~BlockCacheReinsertionPolicy() = default;

    virtual bool shouldReinsert(StringRef key) = 0;
};
}
