#include <Functions/FunctionsConversion.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
}

class FunctionPeriodAdd : public IFunction
{
public:
    static constexpr auto name = "period_add";
    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionPeriodAdd>();
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

        if (!(isNumber(arguments[1]) || isStringOrFixedString(arguments[1])))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be a number or a string", getName());

        // The year-month value follows a non-negative format of "YYYY-MM", ensuring that the value is always non-negative
        // As a result, the maximum representable value is "9999-11", which can be efficiently stored within 20 bits
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & periods = arguments[0].column;
        const auto & months = arguments[1].column;

        const auto periods_is_string = isStringOrFixedString(arguments[0].type); // to determine whether a conversion is required
        const auto months_is_string = isStringOrFixedString(arguments[1].type); // to determine whether a conversion is required

        auto res = ColumnUInt32::create(input_rows_count);
        auto & res_data = res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int period;
            if (periods_is_string)
                period = toPeriodFromString(periods->getDataAt(i));
            else
                period = periods->getInt(i);

            if (period < 0)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Year cannot be negative");

            int num_months;
            if (months_is_string)
                num_months = toNumber(months->getDataAt(i));
            else
                num_months = std::round(months->getFloat32(i));

            int period_year = period / 100;
            int period_month = period % 100;

            int total_months = period_year * 12 + period_month + num_months;

            int result_year = total_months / 12;
            int result_month = total_months % 12;

            int result = result_month ? result_year * 100 + result_month : (result_year - 1) * 100 + 12;
            res_data[i] = (result >= 0) ? result : 0;
        }
        return res;
    }

private:
    inline int toPeriodFromString(StringRef s) const
    {
        // Throw an exception if the size is not 6 because it violates the YYYYMM date format
        if (s.size != 6)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for period. Should be YYYYMM");
        return toNumber(s);
    }

    inline int toNumber(StringRef s) const
    {
        // This function will be called twice for each row
        // TODO: Optimize if possible
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

void registerFunctionPeriodAdd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPeriodAdd>(FunctionFactory::CaseInsensitive);
}

}