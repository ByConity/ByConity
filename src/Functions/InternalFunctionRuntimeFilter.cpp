#include <Functions/InternalFunctionRuntimeFilter.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

void registerInternalFunctionRuntimeFilter(FunctionFactory & factory)
{
    factory.registerFunction<InternalFunctionRuntimeFilter>();
}

}
