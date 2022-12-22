#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFiveMinute = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinuteImpl>;

void registerFunctionToStartOfFiveMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfFiveMinute>();
    factory.registerAlias("toStartOfFiveMinutes", FunctionToStartOfFiveMinute::name, FunctionFactory::CaseInsensitive);
}

}
