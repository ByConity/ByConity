#pragma once

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
/**
 * internal function for optimizer
 */
class InternalFunctionDynamicFilter : public IFunction
{
public:
    static constexpr auto name = "$dynamicFilter";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<InternalFunctionDynamicFilter>(); }

    String getName() const override { return name; }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        throw Exception("Unexpected internal function: dynamic filter", ErrorCodes::NOT_IMPLEMENTED);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt8>(); }

    size_t getNumberOfArguments() const override { return 4; }
};

void registerInternalFunctionDynamicFilter(FunctionFactory &);
}
