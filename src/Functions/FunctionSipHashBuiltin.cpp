#include <Functions/FunctionSipHashBuiltin.h>
#include <Common/register_objects.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(SipHashBuiltin)
{
    factory.registerFunction<FunctionSipHashBuiltin>(FunctionSipHashBuiltin::name, FunctionFactory::CaseSensitive);
}
}
