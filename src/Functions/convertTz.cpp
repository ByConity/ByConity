#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionConvertTz : public IFunction
{
public:
    static constexpr auto name = "convert_tz";

    explicit FunctionConvertTz(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionConvertTz>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        return std::make_shared<DataTypeDateTime>("UTC");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto executeFunction = [&](const std::string & function_name, const ColumnsWithTypeAndName input, const DataTypePtr output_type) {
            auto func = FunctionFactory::instance().get(function_name, context);
            return func->build(input)->execute(input, output_type, input_rows_count);
        };

        ColumnsWithTypeAndName date{arguments[0]};
        auto origin = executeFunction("toDateTime", date, std::make_shared<DataTypeDateTime>("UTC"));

        ColumnPtr from = arguments[1].column;
        ColumnPtr to = arguments[2].column;

        DataTypePtr number_type = std::make_shared<DataTypeNumber<Int32>>();
        MutableColumnPtr diff_col{number_type->createColumn()};
        auto & internal_data = dynamic_cast<ColumnVector<Int32> *>(diff_col.get())->getData();
        internal_data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            int diff = checkAndGetMinutes(to->getDataAt(i)) - checkAndGetMinutes(from->getDataAt(i));
            internal_data[i] = diff;
        }

        ColumnPtr diff_operand(std::move(diff_col));
        ColumnsWithTypeAndName operands
            = {{origin, std::make_shared<DataTypeDateTime>("UTC"), "date"}, {diff_operand, number_type, "diff"}};

        auto result = executeFunction("addMinutes", operands, result_type);
        return result;
    }

private:
    ContextPtr context;

    inline int checkAndGetMinutes(StringRef s) const
    {
        if (s.size != 6)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for timezone. Should be '+HH:MM or -HH:MM'");

        int sign;
        if (s.data[0] == '+')
            sign = 1;
        else if (s.data[0] == '-')
            sign = -1;
        else
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for timezone. Should be '+HH:MM or -HH:MM'");

        if (!isdigit(s.data[1]) || !isdigit(s.data[2]))
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for timezone. Should be '+HH:MM or -HH:MM'");
        int hour = (s.data[1] - '0') * 10 + s.data[2] - '0';

        if (s.data[3] != ':')
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for timezone. Should be '+HH:MM or -HH:MM'");

        if (!isdigit(s.data[4]) || !isdigit(s.data[5]))
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE, "Illegal value for timezone. Should be '+HH:MM or -HH:MM'");
        int min = (s.data[4] - '0') * 10 + s.data[5] - '0';

        return sign * (hour * 60 + min);
    }
};

REGISTER_FUNCTION(ConvertTz)
{
    factory.registerFunction<FunctionConvertTz>(FunctionFactory::CaseInsensitive);
}

}
