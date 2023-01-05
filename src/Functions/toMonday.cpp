#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToMonday = FunctionDateOrDateTimeToDateOrDate32<ToMondayImpl>;

void registerFunctionToMonday(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToMonday>();
}

}


