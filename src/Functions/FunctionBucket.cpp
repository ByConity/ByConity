#include <Functions/FunctionBucket.h>
#include <Common/register_objects.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(Bucket)
{
    factory.registerFunction<FunctionBucketOverloadResolver>();
}
}
