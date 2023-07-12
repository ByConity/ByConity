#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionUTCTimestamp : public IFunction
{
public:
    static constexpr auto name = "utc_timestamp";

    explicit FunctionUTCTimestamp(ContextPtr context_) : context(context_)
    {
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionUTCTimestamp>(context);
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 0)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        return std::make_shared<DataTypeDateTime>("UTC");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DataTypePtr now_input_type = std::make_shared<DataTypeString>();
        auto timezone_col = now_input_type->createColumnConst(input_rows_count, Field("UTC"));
        ColumnsWithTypeAndName now_input = {{timezone_col, now_input_type, "now"}};

        auto now_func = FunctionFactory::instance().get("now", context);
        auto result = now_func->build(now_input)->execute(now_input, result_type, input_rows_count);
        return result;
    }

private:
    ContextPtr context;
};

void registerFunctionUTCTimestamp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUTCTimestamp>(FunctionFactory::CaseInsensitive);
}

}
