#include <cstddef>
#include <string>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsConversion.h>
#include <IO/WriteHelpers.h>
#include "common/types.h"
#include <common/range.h>
#include "Columns/ColumnNullable.h"
#include "Columns/IColumn.h"
#include "Core/Types.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

#define APPLY_FOR_EACH_NUMERIC_TYPE(V) \
    V(UInt8  ) \
    V(UInt16 ) \
    V(UInt32 ) \
    V(UInt64 ) \
    V(UInt128 ) \
    V(UInt256 ) \
    V(Int8   ) \
    V(Int16  ) \
    V(Int32  ) \
    V(Int64  ) \
    V(Int128 ) \
    V(Int256  )
class FunctionExportSet : public IFunction
{
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();

public:
    static constexpr auto name = "export_set";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionExportSet>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > 5)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most 5",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isInteger(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isNumber(arguments[4]))
            throw Exception(
                "Illegal type " + arguments[4]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        #define V(TYPE) \
        if (checkColumn<ColumnVector<TYPE>>(arguments[0].column.get())) \
        { \
            return execute<TYPE>(arguments); \
        }
        APPLY_FOR_EACH_NUMERIC_TYPE(V)
        #undef V

        return ColumnString::create();
    }
private:
    template <typename T>
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr bits_col = arguments[0].column;
        ColumnPtr length_col = arguments[4].column;
        size_t size = bits_col->size();

        ColumnPtr on_column = arguments[1].column;
        if (!isString(arguments[1].type))
        {
            ColumnsWithTypeAndName args_i {arguments[1]};
            on_column = ConvertImplGenericToString::execute(args_i);
        }

        ColumnPtr off_column = arguments[2].column;
        if (!isString(arguments[2].type))
        {
            ColumnsWithTypeAndName args_i {arguments[2]};
            off_column = ConvertImplGenericToString::execute(args_i);
        }

        ColumnPtr separator_column = arguments[3].column;
        if (!isString(arguments[3].type))
        {
            ColumnsWithTypeAndName args_i {arguments[3]};
            separator_column = ConvertImplGenericToString::execute(args_i);
        }

        auto col_res = ColumnString::create();
        ColumnString::Chars & res_data = col_res->getChars();
        ColumnString::Offsets & res_offsets = col_res->getOffsets();
        res_offsets.resize(size);
        size_t prev_res_offset = 0;
        size_t res_offset = 0;


        const ColumnVector<T> * col_numeric = checkAndGetColumn<ColumnVector<T>>(bits_col.get());
        auto & vec_bits = col_numeric->getData();

        for (size_t i = 0; i < vec_bits.size(); ++i)
        {
            size_t res_size = 0;
            auto bits_val = vec_bits[i];
            UInt64 length_val = length_col->get64(i);

            if (length_val < 1 || length_val > 64)
                length_val = 64;

            auto on_str = on_column->getDataAt(i);
            auto off_str = off_column->getDataAt(i);
            auto separator_str = separator_column->getDataAt(i);

            size_t max_size = std::max(on_str.size, off_str.size) * length_val;
            res_data.reserve(res_data.size() + max_size + 1);

            UInt8 * src_data = reinterpret_cast<UInt8 *>(&res_data[prev_res_offset]);
            for (size_t k = 0; k < length_val; k++)
            {
                int mask =  1 << k;
                int masked_val = bits_val & mask;
                int curr_bit = masked_val >> k;
                if (curr_bit)
                {
                    memcpy(src_data + res_size, reinterpret_cast<const UInt8 *>(on_str.data), on_str.size);
                    res_size += on_str.size;
                }
                else
                {
                    memcpy(src_data + res_size, reinterpret_cast<const UInt8 *>(off_str.data), off_str.size);
                    res_size += off_str.size;
                }
                if (k < length_val - 1)
                {
                    memcpy(src_data + res_size, reinterpret_cast<const UInt8 *>(separator_str.data), separator_str.size);
                    res_size += separator_str.size;
                }
            }

            res_data.resize(res_data.size() + res_size + 1);
            res_offset += (res_size + 1);
            res_data[res_offset - 1] = '\0';
            res_offsets[i] = res_offset;
            prev_res_offset = res_offsets[i];
        }

        return col_res;
    }
};

void registerFunctionExportSet(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExportSet>(FunctionFactory::CaseInsensitive);
}

}
