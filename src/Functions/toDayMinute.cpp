#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToDayMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToDayMinuteImpl>;

void registerFunctionToDayMinute(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToDayMinute>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayMinute>("DAY_MINUTE", FunctionFactory::CaseInsensitive);
}

}
