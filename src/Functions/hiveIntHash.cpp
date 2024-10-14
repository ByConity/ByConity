#include <cstdlib>
#include <cstdint>
#include <Functions/hiveIntHash.h>

namespace DB::HiveIntHash
{

static const BigInteger k0 = stringToBigInteger("18397679294719823053");
static const BigInteger k1 = stringToBigInteger("14181476777654086739");

BigInteger intHash64(BigInteger x)
{
    x ^= x >> 33;
    x *= k0;
    x ^= x >> 33;
    x *= k1;
    x ^= x >> 33;
    return x;
}
}
