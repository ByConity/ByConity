#include <Functions/InternalFunctionRuntimeFilter.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(InternalFunctionRuntimeFilter)
{
    factory.registerFunction<InternalFunctionRuntimeFilter>();
}

}
