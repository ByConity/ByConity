#pragma once

#include <cstddef>

namespace DB::TSO
{

constexpr static size_t TSO_BITS = 64;
constexpr static size_t LOGICAL_BITS = 18;
constexpr static size_t MAX_LOGICAL = 1 << LOGICAL_BITS;
constexpr static size_t LOGICAL_BITMASK = 0x3FFFF;
constexpr static size_t TSO_UPDATE_INTERVAL = 50;  /// 50 milliseconds

#define ts_to_physical(ts) ((ts) >> LOGICAL_BITS)
#define ts_to_logical(ts) ((ts) & LOGICAL_BITMASK)
#define physical_logical_to_ts(physical, logical) (((physical) << LOGICAL_BITS) | ((logical) & LOGICAL_BITMASK))

}
