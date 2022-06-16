#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>

namespace DB
{
struct FieldWithType
{
    DataTypePtr type;
    Field value;
};

using FieldsWithType = std::vector<FieldWithType>;

/**
 * FunctionInvoker is a util to execute a function by the function's name and arguments.
 */
class FunctionInvoker
{
public:
    static FieldWithType execute(const String & function_name, const FieldsWithType & arguments, ContextPtr context);
    static FieldWithType execute(const String & function_name, const ColumnsWithTypeAndName & arguments, ContextPtr context);
};
}
