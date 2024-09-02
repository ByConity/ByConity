#include <Functions/FunctionsConversion.h>
#include <Functions/IFunctionMySql.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
}

namespace {

enum class Kind
{
    PERIOD_ADD,
    PERIOD_DIFF
};

template <Kind type>
class FunctionPeriod : public IFunction
{
public:
    static constexpr auto name = type == Kind::PERIOD_ADD ? "period_add" : "period_diff";
    static FunctionPtr create(ContextPtr /*context*/)
    {
        /// period_add is a mysql function, no need to judge dialect here
        return std::make_shared<IFunctionMySql>(std::make_unique<FunctionPeriod<type>>());
    }

    ArgType getArgumentsType() const override { return ArgType::NUMBERS; }

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

        if (!isNumber(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an number in the form YYYY-MM",
                getName());

        if (!isNumber(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be a number", getName());

        return std::make_shared<DataTypeInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & lhs_col = arguments[0].column;
        const auto & rhs_col = arguments[1].column;

        auto res = ColumnInt32::create(input_rows_count);
        auto & res_data = res->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int lhs = getMonthFromPeriod(lhs_col->getInt(i));
            int rhs = 0;
            if constexpr (type == Kind::PERIOD_ADD)
            {
                rhs = std::round(rhs_col->getFloat32(i));
                res_data[i] = getPeriodFromMonth(lhs + rhs);
            }
            else
            {
                rhs = getMonthFromPeriod(rhs_col->getInt(i));
                res_data[i] = lhs - rhs;
            }
        }
        return res;
    }
private:
    inline int getMonthFromPeriod(int period) const
    {
        if (period < 0)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Period cannot be negative");

        int year = period / 100;
        int month = period % 100;

        if constexpr (type == Kind::PERIOD_DIFF)
        {
            if (year == 0)
                return month;
        }
        /// MySQL able to handle 2 digit of year
        /// [0, 69] will be 20xx
        /// [70, 99] will be 19xx
        if (year <= 69)
            year += 2000;
        else if (year < 100)
            year += 1900;
        return year * 12 + month;
    }

    inline int getPeriodFromMonth(int months) const
    {
        int year = months / 12;
        int month = months % 12;

        return month ? year * 100 + month : (year - 1) * 100 + 12;
    }
};
}

REGISTER_FUNCTION(PeriodMath)
{
    factory.registerFunction<FunctionPeriod<Kind::PERIOD_ADD>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionPeriod<Kind::PERIOD_DIFF>>(FunctionFactory::CaseInsensitive);
}

}
