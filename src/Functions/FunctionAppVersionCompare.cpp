#include <Functions/FunctionFactory.h>
#include <Functions/FunctionAppVersionCompare.h>

namespace DB
{

void registerFunctionsAppVersionCompare(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVersionCompare>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionAppVersionCompare>(FunctionFactory::CaseInsensitive);
}

}
