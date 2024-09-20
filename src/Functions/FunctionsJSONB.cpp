#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsJSONB.h>

namespace DB
{
REGISTER_FUNCTION(JSONB)
{
    factory.registerFunction<FunctionJSONBuildArray>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionJSONObject>(FunctionFactory::CaseInsensitive);
}
} // namespace DB
