#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionMySql.h>
#include <IO/WriteHelpers.h>
#include <common/range.h>

#include <memory>
#include <string>
#include <vector>

#include <Functions/formatNumber.h>
#include <Functions/formatString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <typename Name>
class FormatFunction : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();
    bool mysql_mode = false;
    ContextPtr context;
    FormatFunction(ContextPtr context_) : context(context_)
    {
        mysql_mode = context->getSettingsRef().dialect_type == DialectType::MYSQL;
    }

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FormatFunction>(context));
        return std::make_shared<FormatFunction>(context);
    }

    ArgType getArgumentsType() const override { return ArgType::NUM_NUM_STR; }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override
    {
        // MySQL format function uses default implementation for constants
        return mysql_mode;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if (mysql_mode)
            return {1, 2};
        else
            return {0};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > FormatStringImpl::argument_threshold)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most " + std::to_string(FormatStringImpl::argument_threshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (mysql_mode)
        {
            // MySQL compatibility: select format(X, D, locale) -> format number X to D decimal places with comma
            if (arguments.size() != 2 && arguments.size() != 3)
                throw Exception(
                    getName() + " only support 2-3 arguments, the number of passed arguments: " + toString(arguments.size()),
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            if (!isNumber(arguments[0]))
                throw Exception(
                    "Illegal type " + arguments[0]->getName() + " of argument 0 of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!isNumber(arguments[1]))
                throw Exception(
                    "Illegal type " + arguments[1]->getName() + " of argument 1 of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (arguments.size() == 3 && !isString(arguments[2]))
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of argument 2 of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return std::make_shared<DataTypeString>();
        }

        // clickhouse format string function
        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!isStringOrFixedString(arg))
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {

        if (mysql_mode)
        {
            return FormatNumberImpl::formatExecute(arguments, return_type, input_rows_count, context);
        }
        const ColumnPtr & c0 = arguments[0].column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);

        if (!c0_const_string)
            throw Exception("First argument of function " + getName() + " must be constant string", ErrorCodes::ILLEGAL_COLUMN);

        String pattern = c0_const_string->getValue<String>();

        auto col_res = ColumnString::create();

        std::vector<const ColumnString::Chars *> data(arguments.size() - 1);
        std::vector<const ColumnString::Offsets *> offsets(arguments.size() - 1);
        std::vector<size_t> fixed_string_sizes(arguments.size() - 1);
        std::vector<String> constant_strings(arguments.size() - 1);

        bool has_column_string = false;
        bool has_column_fixed_string = false;
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const ColumnPtr & column = arguments[i].column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                has_column_string = true;
                data[i - 1] = &col->getChars();
                offsets[i - 1] = &col->getOffsets();
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                has_column_fixed_string = true;
                data[i - 1] = &fixed_col->getChars();
                fixed_string_sizes[i - 1] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[i - 1] = const_col->getValue<String>();
            }
            else
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        FormatStringImpl::formatExecute(
            has_column_string,
            has_column_fixed_string,
            std::move(pattern),
            data,
            offsets,
            fixed_string_sizes,
            constant_strings,
            col_res->getChars(),
            col_res->getOffsets(),
            input_rows_count);

        return col_res;
    }
};


struct NameFormat
{
    static constexpr auto name = "format";
};
using FunctionFormat = FormatFunction<NameFormat>;

}

REGISTER_FUNCTION(Format)
{
    factory.registerFunction<FunctionFormat>(FunctionFactory::CaseInsensitive);
}

}
