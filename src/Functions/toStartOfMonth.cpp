#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfMonthImpl>;
using FunctionToStartOfBiMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfBiMonthImpl>;

void registerFunctionToStartOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMonth>();
    factory.registerFunction<FunctionToStartOfBiMonth>();
}

}
