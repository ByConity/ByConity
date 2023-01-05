#include <Functions/InternalFunctionsDynamicFilter.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

void registerInternalFunctionDynamicFilter(FunctionFactory & factory)
{
    factory.registerFunction<InternalFunctionDynamicFilter>();
}

}
