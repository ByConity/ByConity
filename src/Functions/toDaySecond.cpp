#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToDaySecond = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToDaySecondImpl>;

REGISTER_FUNCTION(ToDaySecond)
{
    factory.registerFunction<FunctionToDaySecond>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDaySecond>("DAY_SECOND", FunctionFactory::CaseInsensitive);
}

}
