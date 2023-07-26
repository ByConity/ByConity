#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToDayHour = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayHourImpl>;

void registerFunctionToDayHour(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToDayHour>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayHour>("DAY_HOUR", FunctionFactory::CaseInsensitive);
}

}
