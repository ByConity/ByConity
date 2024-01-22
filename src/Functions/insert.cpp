#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

class FunctionInsert : public IFunction
{
public:
    static constexpr auto name = "insert";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionInsert>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
        {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isNumber(arguments[1]))
        {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isNumber(arguments[2]))
        {
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isStringOrFixedString(arguments[3]))
        {
            throw Exception(
                "Illegal type " + arguments[3]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column_src = arguments[0].column;
        const ColumnPtr column_start_pos = arguments[1].column;
        const ColumnPtr column_len = arguments[2].column;
        const ColumnPtr column_replacement = arguments[3].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            vector(
                col->getChars(),
                col->getOffsets(),
                column_start_pos,
                column_len,
                column_replacement,
                col_res->getChars(),
                col_res->getOffsets());
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            vectorFixed(
                col_fixed->getChars(),
                col_fixed->getN(),
                column_start_pos,
                column_len,
                column_replacement,
                col_res->getChars(),
                col_res->getOffsets());
            return col_res;
        }
        else
        {
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }

private:
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const ColumnPtr column_start_pos,
        const ColumnPtr column_len,
        const ColumnPtr column_replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t res_offset = 0;
        size_t start, len, src_size, dest_size, start_mask;
        StringRef replacement;

        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * src_data = (i > 0) ? &data[offsets[i - 1]] : &data[0];
            src_size = (i > 0) ? offsets[i] - offsets[i - 1] - 1 : offsets[i] - 1;
            UInt8 * dest_data = &res_data[res_offsets[i - 1]];
            dest_size = 0;

            /// Get start position as float and round to handle floats and decimals
            /// Cast to unsigned to make negative number really large
            /// Possible flaw: input string has length > INT64_MAX but not possible currently
            start = static_cast<size_t>(std::round(column_start_pos->getFloat64(i)));
            /// A mask that set too large start pos to 0 and will not affect smaller ones
            start_mask = ~static_cast<size_t>(start > src_size);
            start &= start_mask;

            len = std::min(static_cast<size_t>(std::round(column_len->getFloat64(i))), src_size);
            replacement = column_replacement->getDataAt(i);

            if (start && start <= src_size)
            {
                // Copy data from src to dest before the start index
                memcpy(dest_data, src_data, start - 1);
                dest_size += (start - 1);

                // Copy the replacement string data to dest
                memcpy(dest_data + dest_size, replacement.data, replacement.size);
                dest_size += replacement.size;

                // Copy remaining data from src to dest if exists
                if (start + len <= src_size)
                {
                    memcpy(dest_data + dest_size, src_data + start + len - 1, src_size - (start + len) + 1);
                    dest_size += (src_size - (start + len) + 1);
                }
            }
            else
            {
                memcpy(dest_data, src_data, src_size);
                dest_size = src_size;
            }

            res_data.resize(res_data.size() + dest_size + 1);
            res_offset += dest_size + 1;
            res_data[res_offset - 1] = '\0';
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        const ColumnPtr column_start_pos,
        const ColumnPtr column_len,
        const ColumnPtr column_replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t size = (data.size() / n);
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_res_offset = 0;
        size_t res_offset = 0;
        size_t start, len, start_mask;
        StringRef replacement;

        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * src_data = &data[i * n];
            size_t src_size = n;
            UInt8 * dest_data = &res_data[prev_res_offset];
            size_t dest_size = 0;

            /// Get start position as float and round to handle floats and decimals
            /// Cast to unsigned to make negative number really large
            /// Possible flaw: input string has length > INT64_MAX but not possible currently
            start = static_cast<size_t>(std::round(column_start_pos->getFloat64(i)));
            start_mask = ~static_cast<size_t>(start > src_size);
            start &= start_mask;
            /// A mask that set too large start pos to 0 and will not affect smaller ones
            len = std::min(static_cast<size_t>(std::round(column_len->getFloat64(i))), src_size);

            replacement = column_replacement->getDataAt(i);

            if (start && start <= src_size)
            {
                // Copy data from src to dest before the start index
                memcpy(dest_data, src_data, start - 1);
                dest_size += (start - 1);

                // Copy the replacement string data to dest
                memcpy(dest_data + dest_size, replacement.data, replacement.size);
                dest_size += replacement.size;

                // Copy remaining data from src to dest if exists
                if (start + len <= src_size)
                {
                    memcpy(dest_data + dest_size, src_data + start + len - 1, src_size - (start + len) + 1);
                    dest_size += (src_size - (start + len) + 1);
                }
            }
            else
            {
                memcpy(dest_data, src_data, src_size);
                dest_size = src_size;
            }

            res_data.resize(res_data.size() + dest_size + 1);
            res_offset += dest_size + 1;
            res_data[res_offset - 1] = '\0';
            res_offsets[i] = res_offset;
            prev_res_offset = res_offsets[i];
        }
    }
};

REGISTER_FUNCTION(FunctionsInsert)
{
    factory.registerFunction<FunctionInsert>(FunctionFactory::CaseInsensitive);
}

}
