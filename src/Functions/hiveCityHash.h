#pragma once
#include <cstdlib>
#include <cstdint>

/// A C++ implementation for the cityHash64 used by the Hive.
/// Check https://bytedance.feishu.cn/docs/doccnpa7GeSnTBp3dRASP8mT13c

namespace DB::HiveCityHash
{

typedef int32_t Int32;
typedef int64_t Int64;
typedef uint64_t UInt64;

// Hash function for a byte array.
UInt64 cityHash64(const char* s, int pos, int len);

}
