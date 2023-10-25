#include <Functions/FunctionsConversion.h>

#define PERIOD_TO_MONTHS(period) (((period) / 100) * 12 + (period) % 100)

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
}

class FunctionPeriodDiff : public IFunction
{
public:
    static constexpr auto name = "period_diff";
    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionPeriodDiff>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        if (!(isInteger(arguments[0]) || isStringOrFixedString(arguments[0])))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an integer or a string in the form YYYY-MM",
                getName());

        if (!(isInteger(arguments[1]) || isStringOrFixedString(arguments[1])))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be an integer or a string", getName());

        // The largest difference between 2 valid periods is 9999 years and 12 months, which is 119999 months
        // As a result, the maximum representable value is ±120000, which can be efficiently stored within 17 bits
        return std::make_shared<DataTypeInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & lhs_col = arguments[0].column;
        const auto & rhs_col = arguments[1].column;

        const auto lhs_is_string = isStringOrFixedString(arguments[0].type); // to determine whether a conversion is required
        const auto rhs_is_string = isStringOrFixedString(arguments[1].type); // to determine whether a conversion is required

        auto res = ColumnInt32::create(input_rows_count);
        auto & res_data = res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int lhs;
            if (lhs_is_string)
                lhs = PERIOD_TO_MONTHS(getPeriodFromString(lhs_col->getDataAt(i)));
            else
                lhs = PERIOD_TO_MONTHS(lhs_col->getInt(i));

            int rhs;
            if (rhs_is_string)
                rhs = PERIOD_TO_MONTHS(getPeriodFromString(rhs_col->getDataAt(i)));
            else
                rhs = PERIOD_TO_MONTHS(rhs_col->getInt(i));

            if (lhs < 0 || rhs < 0)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for period. Should be YYYY-MM and cannot be negative");

            res_data[i] = lhs - rhs;
        }
        return res;
    }

private:
    inline int getPeriodFromString(StringRef s) const
    {
        if (s.size != 6) // format is not YYYY-MM
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for period. Should be YYYY-MM");

        try
        {
            return std::round(std::stod(s.toString()));
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value {}", s.toString());
        }
    }
};

REGISTER_FUNCTION(PeriodDiff)
{
    factory.registerFunction<FunctionPeriodDiff>(FunctionFactory::CaseInsensitive);
}

}
