#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB {


class FunctionSipHashBuiltin : public IFunction
{
public:
    static constexpr auto name = "sipHashBuitin";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSipHashBuiltin>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & ) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt64::create(input_rows_count, 0);
        auto & result_date = result_column->getData();
        for (size_t i = 0; i < input_rows_count; i++)
        {
            SipHash hash;
            for (const auto & argument : arguments)
            {
                argument.column->updateHashWithValue(i, hash);
            }
            result_date[i] = hash.get64();
        }
        return result_column;
    }
};

}

