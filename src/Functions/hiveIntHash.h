#pragma once

#include <cstdlib>
#include <cstdint>
#include <BigIntegerLibrary.hh>

/// A C++ implementation for the intHash64 used by the Hive.
/// Check https://bytedance.feishu.cn/docs/doccnpa7GeSnTBp3dRASP8mT13c

namespace DB::HiveIntHash
{

static const BigInteger k0 = stringToBigInteger("18397679294719823053");
static const BigInteger k1 = stringToBigInteger("14181476777654086739");

BigInteger intHash64(BigInteger x) {
    x ^= x >> 33;
    x *= k0;
    x ^= x >> 33;
    x *= k1;
    x ^= x >> 33;
    return x;
}

};
