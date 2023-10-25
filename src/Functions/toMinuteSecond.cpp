#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToMinuteSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToMinuteSecondImpl>;

REGISTER_FUNCTION(ToMinuteSecond)
{
    factory.registerFunction<FunctionToMinuteSecond>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToMinuteSecond>("MINUTE_SECOND", FunctionFactory::CaseInsensitive);
}

}
