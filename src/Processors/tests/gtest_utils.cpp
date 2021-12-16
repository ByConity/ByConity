#include <cstddef>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/tests/gtest_utils.h>
#include "common/types.h"
namespace UnitTest
{
using namespace DB;

Chunk createUInt8Chunk(size_t row_num, size_t column_num, UInt8 value)
{
    Columns columns;
    for (size_t i = 0; i < column_num; i++)
    {
        auto col = ColumnUInt8::create(row_num, value);
        columns.emplace_back(std::move(col));
    }
    return Chunk(std::move(columns), row_num);
}

Block createUInt64Block(size_t row_num, size_t column_num, UInt8 value)
{
    ColumnsWithTypeAndName cols;
    for (size_t i = 0; i < column_num; i++)
    {
        auto column = ColumnUInt64::create(row_num, value);
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), "column" + std::to_string(i));
    }
    return Block(cols);
}


ExecutableFunctionPtr createRepartitionFunction(ContextPtr context, const ColumnsWithTypeAndName & arguments)
{
    const String repartition_func_name = "cityHash64";
    auto & factory = FunctionFactory::instance();
    auto res = factory.tryGetImpl(repartition_func_name, context);
    if (!res)
    {
        factory.registerFunction<FunctionCityHash64>();
    }
    FunctionOverloadResolverPtr func_builder = factory.get(repartition_func_name, context);
    FunctionBasePtr function_base = func_builder->build(arguments);
    return function_base->prepare(arguments);
}

}
