#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunctionMySql.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

class FunctionELT : public IFunction
{
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();

public:
    static constexpr auto name = "ELT";
    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionELT>());
        return std::make_shared<FunctionELT>();
    }

    ArgType getArgumentsType() const override { return ArgType::INT_STR; }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size < 2)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        if (size > argument_threshold)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at most "
                    + std::to_string(argument_threshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        DataTypePtr arg = arguments[0];
        if (arg->getTypeId() == TypeIndex::Nullable)
        {
            arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
        }
        if (!isNumber(arg) && !isNothing(arg))
        {
            throw Exception{
                "Illegal type " + arg->getName() + " of argument " + std::to_string(0) + " of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        // handle null only index
        if (arguments[0].column->onlyNull())
        {
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        }

        MutableColumnPtr res{result_type->createColumn()};
        res->reserve(input_rows_count);

        ColumnNullable & nullable_column = assert_cast<ColumnNullable &>(*res);
        ColumnString * res_nested_col = typeid_cast<ColumnString *>(&nullable_column.getNestedColumn());
        ColumnString::Chars & res_data = res_nested_col->getChars();
        ColumnString::Offsets & res_offset = res_nested_col->getOffsets();

        const ColumnPtr col_index = arguments[0].column;
        size_t arg_size = arguments.size();
        // extract data columns and null maps if exist
        const UInt8 * nullable_args_map[arg_size];
        ColumnPtr raw_column_maps[arg_size];
        extractNullMapAndNestedCol(arguments, raw_column_maps, nullable_args_map);

        res_data.resize(input_rows_count);
        res_offset.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> write_buffer(res_data);
        FormatSettings format_settings;
        size_t offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (unlikely(nullable_args_map[0] && nullable_args_map[0][i]))
            {
                offset += 1;
                writeResultColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 1);
                continue;
            }

            auto index = raw_column_maps[0]->getInt(i);
            // TODO: add optimizations for constant index
            if (unlikely(index < 1 || static_cast<unsigned long>(index) > arg_size - 1 || (nullable_args_map[index] && nullable_args_map[index][i])))
            {
                offset += 1;
                writeResultColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 1);
                continue;
            }

            const IColumn & col_from = *arguments[index].column;
            const IDataType & type = *arguments[index].type;
            // extract the arg at index
            auto serialization = type.getDefaultSerialization();
            serialization->serializeText(col_from, i, write_buffer, format_settings);
            offset = write_buffer.count() + 1;
            writeResultColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 0);
        }

        write_buffer.finalize();

        return res;
    }

private:
    inline void writeResultColumn(
        WriteBufferFromVector<ColumnString::Chars> & write_buffer,
        ColumnString::Offsets & res_offset,
        ColumnNullable & nullable_column,
        char c,
        size_t index,
        size_t offset,
        uint8_t null_flag) const
    {
        writeChar(c, write_buffer);
        res_offset[index] = offset;
        nullable_column.getNullMapData().push_back(null_flag);
    }
};

REGISTER_FUNCTION(Elt)
{
    factory.registerFunction<FunctionELT>(FunctionFactory::CaseInsensitive);
}

}
