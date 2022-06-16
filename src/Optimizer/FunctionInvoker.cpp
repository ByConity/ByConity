#include <Optimizer/FunctionInvoker.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

FieldWithType FunctionInvoker::execute(const String & function_name, const FieldsWithType & arguments, ContextPtr context)
{
    ColumnsWithTypeAndName columns(arguments.size());

    std::transform(arguments.begin(), arguments.end(), columns.begin(), [](auto & arg) {
        auto col = arg.type->createColumnConst(1, arg.value);
        return ColumnWithTypeAndName(col, arg.type, "");
    });

    return execute(function_name, columns, context);
}

FieldWithType FunctionInvoker::execute(const String & function_name, const ColumnsWithTypeAndName & arguments, ContextPtr context)
{
    auto function_builder = FunctionFactory::instance().get(function_name, context);

    FunctionBasePtr function_base = function_builder->build(arguments);
    auto result_type = function_base->getResultType();
    auto result_column = function_base->execute(arguments, result_type, 1);

    if (!result_column || result_column->size() != 1)
        throw Exception("Invalid result.", ErrorCodes::LOGICAL_ERROR);

    return {result_type, (*result_column)[0]};
}

}
