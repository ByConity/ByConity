#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

#define BIT_CHECK(bitmap, pos) ((bitmap) & (1 << (pos)))
#define NULLABLE_ARG_CHECK(i, j) (nullable_args_map[j] && nullable_args_map[j][i])

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

class FunctionMakeSet : public IFunction
{
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();

public:
    static constexpr auto name = "make_set";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionMakeSet>(); }

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
        if (arguments[0].column->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        MutableColumnPtr res{result_type->createColumn()};
        res->reserve(input_rows_count);

        ColumnNullable & nullable_column = assert_cast<ColumnNullable &>(*res);
        ColumnString * res_nested_col = typeid_cast<ColumnString *>(&nullable_column.getNestedColumn());
        ColumnString::Chars & res_data = res_nested_col->getChars();
        ColumnString::Offsets & res_offset = res_nested_col->getOffsets();

        const ColumnPtr col_bitmaps = arguments[0].column;
        size_t arg_size = arguments.size();
        // extract data columns and null maps if exist
        const UInt8 * nullable_args_map[arg_size];
        ColumnPtr raw_column_maps[arg_size];
        extractNullMapAndNestedCol(arguments, raw_column_maps, nullable_args_map);

        // Using coefficient 2 for initial size is arbitrary.
        res_data.resize(input_rows_count * 2);
        res_offset.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> write_buffer(res_data);
        FormatSettings format_settings;
        size_t offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (unlikely(NULLABLE_ARG_CHECK(i, 0)))
            {
                offset++;
                writeNullableColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 1);
                continue;
            }

            Int64 bitmap = col_bitmaps->getInt(i);
            
            if (fillResult(nullable_args_map, arguments, bitmap, arg_size, i, write_buffer, format_settings))
            {
                offset = write_buffer.count() + 1;
                writeNullableColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 0);   
            }
            else
            {
                offset++;
                writeNullableColumn(write_buffer, res_offset, nullable_column, '\0', i, offset, 1);
            }

        }

        write_buffer.finalize();
        return res;
    }

private:
    size_t countAndGetSize(const UInt8 ** nullable_args_map, const ColumnsWithTypeAndName & arguments, Int64 bitmap, size_t arg_size, size_t row) const
    {
        size_t size = 0;
        bool first = true;

        for (size_t col = 1; col < arg_size && bitmap != 0; col++)
        {
            if (!BIT_CHECK(bitmap, col-1) || unlikely(NULLABLE_ARG_CHECK(row, col)))
                continue;
            
            if (!first)
                size++;
            else 
                first = false;

            size += arguments[col].column->getDataAt(row).size;
        }
        return size;
    }

    inline void writeNullableColumn(
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

    bool fillResult(const UInt8 ** nullable_args_map, const ColumnsWithTypeAndName & arguments, Int64 bitmap, size_t arg_size, size_t row, WriteBufferFromVector<ColumnString::Chars> & write_buffer, FormatSettings & format_settings) const
    {
        bool first = true;
        bool empty = true;
        for (size_t col = 1; col < arg_size && bitmap != 0; col++)
        {
            if (!BIT_CHECK(bitmap, col-1) || unlikely(NULLABLE_ARG_CHECK(row, col)))
                continue;
            
            if (empty)
                empty = false;

            if (!first)
            {
                write_buffer.write(',');
            } 
            else
            {
                first = false;
            }

            const IColumn & col_from = *arguments[col].column;
            const IDataType & type = *arguments[col].type;
            // extract the arg at index
            auto serialization = type.getDefaultSerialization();
            serialization->serializeText(col_from, row, write_buffer, format_settings);
        }
        
        return !empty;
    }
};

REGISTER_FUNCTION(MakeSet)
{
    factory.registerFunction<FunctionMakeSet>(FunctionFactory::CaseInsensitive);
}

}
