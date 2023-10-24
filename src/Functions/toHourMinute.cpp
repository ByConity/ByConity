#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToHourMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToHourMinuteImpl>;

REGISTER_FUNCTION(ToHourMinute)
{
    factory.registerFunction<FunctionToHourMinute>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToHourMinute>("HOUR_MINUTE", FunctionFactory::CaseInsensitive);
}

}
