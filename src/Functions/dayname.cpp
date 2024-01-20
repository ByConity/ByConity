#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
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


class FunctionDayName : public IFunction
{
public:
    static constexpr auto name = "dayname";

    ContextPtr context_ptr;

    explicit FunctionDayName(ContextPtr context) : context_ptr(context) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDayName>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size != 1)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr arg = arguments[0];

        if (!isDateOrDateTime(arg) && !isStringOrFixedString(arg))
        {
            throw Exception{
                "Illegal type " + arg->getName() + " of argument " + std::to_string(0) + " of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        DataTypePtr week_type = std::make_shared<DataTypeString>();
        auto week_col = week_type->createColumnConst(input_rows_count, Field("weekday"));
        ColumnsWithTypeAndName input_col = {{week_col, week_type, "weekday"}};

        if (isStringOrFixedString(arguments[0].type))
        {
            ColumnsWithTypeAndName tmp{arguments[0]};
            auto to_dt = FunctionFactory::instance().get("toDateTime", context_ptr);
            auto col = to_dt->build(tmp)->execute(tmp, std::make_shared<DataTypeDateTime64>(0), input_rows_count);
            ColumnWithTypeAndName converted_col(col, std::make_shared<DataTypeDateTime64>(0), "unixtime");
            input_col.emplace_back(converted_col);
        }
        else
        {
            input_col.emplace_back(arguments[0]);
        }

        auto to_date_name = FunctionFactory::instance().get("dateName", context_ptr);
        return to_date_name->build(input_col)->execute(input_col, std::make_shared<DataTypeString>(), input_rows_count);
    }
};


REGISTER_FUNCTION(DayName)
{
    factory.registerFunction<FunctionDayName>(FunctionFactory::CaseInsensitive);
}

}
