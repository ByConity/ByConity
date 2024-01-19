#include <iostream>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionLogWithBase : public IFunction
{
public:
    static constexpr auto name = "log_with_base";

    ContextPtr context_;

    FunctionLogWithBase(ContextPtr context) : context_(context) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionLogWithBase>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size != 2)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        for (size_t i = 0; i < size; i++)
        {
            if (!isNumber(arguments[i]))
            {
                throw Exception{
                    "Illegal type " + arguments[i]->getName() + " of argument " + std::to_string(i) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        }
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        // log_a(b) = ln(b)/ln(a)

        const ColumnsWithTypeAndName arg1 = {arguments[0]};
        auto log1_func = FunctionFactory::instance().get("log", context_);
        auto col_base = log1_func->build(arg1)->execute(arg1, std::make_shared<DataTypeFloat64>(), input_rows_count);

        const ColumnsWithTypeAndName arg2 = {arguments[1]};
        auto col_num = log1_func->build(arg2)->execute(arg2, std::make_shared<DataTypeFloat64>(), input_rows_count);

        const ColumnsWithTypeAndName div_arg
            = {{col_num, std::make_shared<DataTypeFloat64>(), "num_col"}, {col_base, std::make_shared<DataTypeFloat64>(), "col_base"}};
        auto div_func = FunctionFactory::instance().get("divide", context_);
        return div_func->build(div_arg)->execute(div_arg, std::make_shared<DataTypeFloat64>(), input_rows_count);
    }
};

REGISTER_FUNCTION(LogWithBase)
{
    factory.registerFunction<FunctionLogWithBase>(FunctionFactory::CaseInsensitive);
}

}
