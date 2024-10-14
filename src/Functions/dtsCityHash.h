#pragma once
#include <cstdlib>
#include <cstdint>

/// A C++ implementation for the cityHash64 used by the DTS/Spark.
/// Reimplementation of repo `spark`, branch `branch-3.2-bd`, commit `66465e1cfe95a0f1313043215344a45a0df71da6`

namespace DB::DTSCityHash
{

using Int32 = int32_t;
using Int64 = int64_t;
using UInt64 = uint64_t;

// Hash function for a byte array.
Int64 cityHash64(const char* s, int pos, int len);

UInt64 asUInt64(Int64 l);

}
