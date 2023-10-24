#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#define SECONDS_TO_EPOCH 62167219200

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionToSeconds : public IFunction
{
public:
    static constexpr auto name = "to_seconds";

    explicit FunctionToSeconds(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionToSeconds>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        if (!isDate(arguments[0]) && !isDate32(arguments[0]) && !isDateTime(arguments[0]) && !isDateTime64(arguments[0])
            && !isStringOrFixedString(arguments[0]) && !isTime(arguments[0]))
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ". Should be a date or a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeNumber<UInt64>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DataTypePtr unsigned_type = std::make_shared<DataTypeNumber<UInt32>>();
        auto unix_timestamp_func = FunctionFactory::instance().get("unix_timestamp", context);
        auto seconds_from_epoch = unix_timestamp_func->build(arguments)->execute(arguments, unsigned_type, input_rows_count);

        auto increment = result_type->createColumnConst(input_rows_count, Field(SECONDS_TO_EPOCH));

        ColumnWithTypeAndName inc_col{increment, result_type, "rhs"};
        ColumnsWithTypeAndName operands = {{seconds_from_epoch, unsigned_type, "lhs"}};
        operands.push_back(inc_col);

        auto add_func = FunctionFactory::instance().get("plus", context);
        auto result = add_func->build(operands)->execute(operands, result_type, input_rows_count);

        return result;
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(ToSeconds)
{
    factory.registerFunction<FunctionToSeconds>(FunctionFactory::CaseInsensitive);
}

}
