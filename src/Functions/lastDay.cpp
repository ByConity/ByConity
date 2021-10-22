#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>

namespace DB
{

using FunctionLastDay = FunctionDateOrDateTimeToSomething<DataTypeDate, LastDayImpl>;

void registerFunctionLastDay(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLastDay>();
    factory.registerAlias("last_day", LastDayImpl::name);
}

}
