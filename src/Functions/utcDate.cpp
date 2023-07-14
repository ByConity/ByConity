#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionUTCDate : public IFunction
{
public:
    static constexpr auto name = "utc_date";

    explicit FunctionUTCDate(ContextPtr context_) : context(context_)
    {
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionUTCDate>(context);
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

        return std::make_shared<DataTypeDate>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto execute_function = [&](const std::string & function_name, const ColumnsWithTypeAndName input, const DataTypePtr output_type) {
            auto func = FunctionFactory::instance().get(function_name, context);
            return func->build(input)->execute(input, output_type, input_rows_count);
        };

        DataTypePtr now_input_type = std::make_shared<DataTypeString>();
        auto timezone_col = now_input_type->createColumnConst(input_rows_count, Field("UTC"));
        ColumnsWithTypeAndName now_input = {{timezone_col, now_input_type, "now"}};

        DataTypePtr now_output_type = std::make_shared<DataTypeDateTime>("UTC");
        auto date = execute_function("now", now_input, now_output_type);

        ColumnsWithTypeAndName to_date_input = {{date, now_output_type, "toDate"}};
        auto result = execute_function("toDate", to_date_input, result_type);
        return result;
    }

private:
    ContextPtr context;
};

void registerFunctionUTCDate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUTCDate>(FunctionFactory::CaseInsensitive);
}

}
