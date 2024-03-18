#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(SQLJSON)
{
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONExists, SQLJSONExistsImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONQuery, SQLJSONQueryImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONValue, SQLJSONValueImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONLength, SQLJSONLengthImpl>>();
    factory.registerFunction<FunctionSQLJSONContains<NameSQLJSONContains>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSQLJSONContainsPath<NameSQLJSONContainsPath>>(FunctionFactory::CaseInsensitive);

}

}
