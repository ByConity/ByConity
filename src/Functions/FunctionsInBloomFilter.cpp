#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsInBloomFilter.h>

namespace DB
{

void registerFunctionsBloomFilter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionInBloomFilter>();
}
}
