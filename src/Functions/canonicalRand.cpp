#include <Common/randomSeed.h>
#include <Functions/canonicalRand.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <pcg-random/pcg_random.hpp>

namespace DB
{

void CanonicalRandImpl::execute(char * output, size_t size)
{
    pcg64_fast rng1(randomSeed());
    pcg64_fast rng2(randomSeed());
    std::uniform_real_distribution<Float64> distribution1(min, max);
    std::uniform_real_distribution<Float64> distribution2(min, max);

    for (const char * end = output + size; output < end; output += 16)
    {
        unalignedStore<Float64>(output, distribution1(rng1));
        unalignedStore<Float64>(output + 8, distribution2(rng2));
    }
}
/// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.

REGISTER_FUNCTION(RandCanonical)
{
    factory.registerFunction<FunctionCanonicalRand>();
    factory.registerAlias("randZeroToOne", "randCanonical", FunctionFactory::CaseInsensitive);
}

}
