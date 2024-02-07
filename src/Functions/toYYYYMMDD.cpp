#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;
using FunctionToYYYYMMDDMySql = FunctionDateOrDateTimeToSomething<DataTypeFloat64, ToYYYYMMDDImplMySql>;

REGISTER_FUNCTION(ToYYYYMMDD)
{
    factory.registerFunction<FunctionToYYYYMMDD>();
    factory.registerFunction<FunctionToYYYYMMDDMySql>();
}

}


