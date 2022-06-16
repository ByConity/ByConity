#pragma once
#include <Core/Types.h>
namespace DB::Statistics
{
namespace ConfigParameters
{
    constexpr UInt64 max_cache_size = 1024UL * 16UL;
    constexpr UInt64 cache_expire_time = 600; // in seconds
}

}
