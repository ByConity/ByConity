#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <common/map.h>
#include <common/range.h>

#include "formatString.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    template <typename Name, bool is_injective>
    class ConcatWithSeparatorImpl : public IFunction
    {
    public:
        static constexpr auto name = Name::name;
        explicit ConcatWithSeparatorImpl(ContextPtr context_) : context(context_) { }

        static FunctionPtr create(ContextPtr context) { return std::make_shared<ConcatWithSeparatorImpl>(context); }

        bool useDefaultImplementationForNulls() const override { return context->getSettingsRef().dialect_type != DialectType::MYSQL; }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }

        size_t getNumberOfArguments() const override { return 0; }

        bool isInjective(const ColumnsWithTypeAndName &) const override { return is_injective; }

        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.empty())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 1",
                    getName(),
                    arguments.size());

            if (arguments.size() > FormatStringImpl::argument_threshold)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be at most " + std::to_string(FormatStringImpl::argument_threshold),
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            assert(!arguments.empty());
            if (arguments.size() == 1)
                return result_type->createColumnConstWithDefaultValue(input_rows_count);

            for (size_t col = 0; col < arguments.size(); ++col)
            {
                if (checkAndGetColumn<ColumnNullable>(*arguments[col].column))
                    return executeNullableImpl(arguments, result_type, input_rows_count);
            }

            // Convert all columns to ColumnString
            ColumnsWithTypeAndName args;
            auto convert = std::make_shared<FunctionToString>();
            auto type_string = std::make_shared<DataTypeString>();
            for (const auto & arg : arguments)
            {
                ColumnPtr col = arg.column;
                if (!isStringOrFixedString(arg.type))
                    col = convert->executeImpl({arg}, type_string, input_rows_count);
                args.emplace_back(col, type_string, arg.name);
            }

            auto c_res = ColumnString::create();
            c_res->reserve(input_rows_count);

            const size_t num_exprs = arguments.size() - 1;
            const size_t num_args = 2 * num_exprs - 1;

            std::vector<const ColumnString::Chars *> data(num_args);
            std::vector<const ColumnString::Offsets *> offsets(num_args);
            std::vector<size_t> fixed_string_sizes(num_args);
            std::vector<String> constant_strings(num_args);

            bool has_column_string = false;
            bool has_column_fixed_string = false;

            const ColumnPtr & separator_column = args[0].column;
            for (size_t i = 0; i < num_exprs; ++i)
            {
                if (i != 0)
                {
                    // Insert separator
                    if (const ColumnString * col = checkAndGetColumn<ColumnString>(separator_column.get()))
                    {
                        has_column_string = true;
                        data[2 * i - 1] = &col->getChars();
                        offsets[2 * i - 1] = &col->getOffsets();
                    }
                    else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(separator_column.get()))
                    {
                        has_column_fixed_string = true;
                        data[2 * i - 1] = &fixed_col->getChars();
                        fixed_string_sizes[2 * i - 1] = fixed_col->getN();
                    }
                    else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(separator_column.get()))
                        constant_strings[2 * i - 1] = const_col->getValue<String>();
                    else
                        throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of argument of function {}",
                            separator_column->getName(),
                            getName());
                }

                const ColumnPtr & column = args[i + 1].column;
                if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
                {
                    has_column_string = true;
                    data[2 * i] = &col->getChars();
                    offsets[2 * i] = &col->getOffsets();
                }
                else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
                {
                    has_column_fixed_string = true;
                    data[2 * i] = &fixed_col->getChars();
                    fixed_string_sizes[2 * i] = fixed_col->getN();
                }
                else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
                    constant_strings[2 * i] = const_col->getValue<String>();
                else
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column->getName(), getName());
            }

            String pattern;
            pattern.reserve(num_args * 2);
            for (size_t i = 0; i < num_args; ++i)
                pattern += "{}";

            FormatStringImpl::formatExecute(
                has_column_string,
                has_column_fixed_string,
                std::move(pattern),
                data,
                offsets,
                fixed_string_sizes,
                constant_strings,
                c_res->getChars(),
                c_res->getOffsets(),
                input_rows_count);
            return std::move(c_res);
        }

        ColumnPtr
        executeNullableImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
        {
            if (arguments[0].column->onlyNull())
                return result_type->createColumnConstWithDefaultValue(input_rows_count);
            /// convert all columns to ColumnString.
            ColumnsWithTypeAndName args;
            auto convert = std::make_shared<FunctionToString>();
            auto type_string = std::make_shared<DataTypeString>();
            for (const auto & arg : arguments)
            {
                ColumnPtr col = arg.column;
                if (arg.column->onlyNull())
                    continue;
                if (!isStringOrFixedString(arg.type))
                    col = convert->executeImpl({arg}, type_string, input_rows_count);
                args.emplace_back(col, type_string, arg.name);
            }


            MutableColumnPtr res{result_type->createColumn()};
            res->reserve(input_rows_count);

            const ColumnPtr col_separator = arguments[0].column;
            size_t arg_size = args.size();
            // extract data columns and null maps if exist
            const UInt8 * nullable_args_map[arg_size];
            ColumnPtr raw_column_maps[arg_size];
            extractNullMapAndNestedCol(args, raw_column_maps, nullable_args_map);

            for (size_t r = 0; r < input_rows_count; ++r)
            {
                if (unlikely(nullableArgCheck(nullable_args_map, r, 0)))
                {
                    res->insertDefault();
                    continue;
                }

                StringRef separator = col_separator->getDataAt(r);
                std::string concatenated = "";
                bool first = true;
                for (size_t c = 1; c < args.size(); ++c)
                {
                    if (unlikely(nullableArgCheck(nullable_args_map, r, c)))
                        continue;

                    if (!first)
                        concatenated.append(separator.data);
                    else
                        first = false;

                    concatenated.append(args[c].column->getDataAt(r).data);
                }
                res->insertData(concatenated.c_str(), concatenated.size());
            }
            return res;
        }

    private:
        ContextPtr context;

        inline bool nullableArgCheck(const UInt8 * nullable_args_map[], size_t row, size_t column) const
        {
            return nullable_args_map[column] && nullable_args_map[column][row];
        }
    };

    struct NameConcatWithSeparator
    {
        static constexpr auto name = "concatWithSeparator";
    };
    struct NameConcatWithSeparatorAssumeInjective
    {
        static constexpr auto name = "concatWithSeparatorAssumeInjective";
    };

    using FunctionConcatWithSeparator = ConcatWithSeparatorImpl<NameConcatWithSeparator, false>;
    using FunctionConcatWithSeparatorAssumeInjective = ConcatWithSeparatorImpl<NameConcatWithSeparatorAssumeInjective, true>;
}

REGISTER_FUNCTION(ConcatWithSeparator)
{
    factory.registerFunction<FunctionConcatWithSeparator>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("concatws", NameConcatWithSeparator::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("concat_ws", NameConcatWithSeparator::name, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConcatWithSeparatorAssumeInjective>();
}

}
