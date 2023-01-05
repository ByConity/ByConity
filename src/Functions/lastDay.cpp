#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>

namespace DB
{

using FunctionLastDay = FunctionDateOrDateTimeToDateOrDate32<ToLastDayOfMonthImpl>;

void registerFunctionLastDay(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLastDay>();
    factory.registerAlias("last_day", ToLastDayOfMonthImpl::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("lastDay", ToLastDayOfMonthImpl::name);
}

}
