#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToHourSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToHourSecondImpl>;

void registerFunctionToHourSecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToHourSecond>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToHourSecond>("HOUR_SECOND", FunctionFactory::CaseInsensitive);
}

}
