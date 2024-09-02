#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(SQLJSON)
{
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONExists, SQLJSONExistsImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONQuery, SQLJSONQueryImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONValue, SQLJSONValueImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONLength, SQLJSONLengthImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONKeys, SQLJSONKeysImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<SQLJSONOverloadResolver<NameSQLJSONExtract, SQLJSONExtractImpl>>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("JSON_SIZE", "JSON_LENGTH", FunctionFactory::CaseInsensitive);
    // factory.registerAlias("JSON_ARRAY_LENGTH", "JSON_LENGTH", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSQLJSONContains<NameSQLJSONContains>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSQLJSONContainsPath<NameSQLJSONContainsPath>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionSQLJSONArrayContains<NameSQLJSONArrayContains>>(FunctionFactory::CaseInsensitive);
}

}
