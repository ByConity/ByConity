#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
using FunctionToYearMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYearMonthImpl>;

REGISTER_FUNCTION(ToYearMonth)
{
    factory.registerFunction<FunctionToYearMonth>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToYearMonth>("YEAR_MONTH", FunctionFactory::CaseInsensitive);
}

}
