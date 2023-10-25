#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToDayHour = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayHourImpl>;

REGISTER_FUNCTION(ToDayHour)
{
    factory.registerFunction<FunctionToDayHour>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayHour>("DAY_HOUR", FunctionFactory::CaseInsensitive);
}

}
