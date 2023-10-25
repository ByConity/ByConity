#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToDayOfWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToDayOfWeekImpl>;
using FunctionToDayOfWeekMySQL = FunctionCustomWeekToSomething<DataTypeUInt8, ToDayOfWeekMySQLImpl>;

REGISTER_FUNCTION(ToDayOfWeek)
{
    factory.registerFunction<FunctionToDayOfWeek>(FunctionFactory::CaseInsensitive);

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayOfWeek>("DAYOFWEEK", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionToDayOfWeekMySQL>("WEEKDAY", FunctionFactory::CaseInsensitive);
}

}


