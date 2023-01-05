#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfMonthImpl>;
using FunctionToStartOfBiMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfBiMonthImpl>;

void registerFunctionToStartOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfMonth>();
    factory.registerFunction<FunctionToStartOfBiMonth>();
}

}
