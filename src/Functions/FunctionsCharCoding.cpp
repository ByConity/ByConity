#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/UTF8Helpers.h>
#include <common/types.h>
#include "Core/Types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

#define APPLY_FOR_EACH_NUMERIC_TYPE(V) \
    V(UInt8) \
    V(UInt16) \
    V(UInt32) \
    V(UInt64) \
    V(UInt128) \
    V(UInt256) \
    V(Int8) \
    V(Int16) \
    V(Int32) \
    V(Int64) \
    V(Int128) \
    V(Int256) \
    V(Float32) \
    V(Float64)

#define APPLY_FOR_EACH_DECIMAL_TYPE(D) \
    D(Decimal32) \
    D(Decimal64) \
    D(Decimal128) \
    D(Decimal256)

struct AsciiImpl
{
    template <typename T>
    static ColumnPtr execute(const T * col)
    {
        auto col_res = ColumnUInt8::create();

        ColumnUInt8::Container & vec_res = col_res->getData();
        vec_res.resize(col->size());

        const ColumnString::Chars & vec_src = col->getChars();

        if constexpr (std::is_same_v<ColumnString, T>)
        {
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                vec_res[i] = static_cast<UInt8>(vec_src[prev_offset]);
                prev_offset = offsets_src[i];
            }
        }
        else if constexpr (std::is_same_v<ColumnFixedString, T>)
        {
            size_t n = col->getN();
            for (size_t i = 0; i < vec_res.size(); i += n)
            {
                vec_res[i] = static_cast<UInt8>(vec_src[i]);
            }
        }

        return col_res;
    }
};

struct OrdImpl
{
    template <typename T>
    static ColumnPtr execute(const T * col)
    {
        auto col_res = ColumnUInt32::create();

        ColumnUInt32::Container & vec_res = col_res->getData();
        vec_res.resize(col->size());

        const ColumnString::Chars & vec_src = col->getChars();
        if constexpr (std::is_same_v<ColumnString, T>)
        {
            const ColumnString::Offsets & offsets_src = col->getOffsets();
            size_t prev_offset = 0;

            for (size_t i = 0; i < vec_res.size(); ++i)
            {
                size_t size = UTF8::seqLength(vec_src[prev_offset]);
                vec_res[i] = 0;
                for (Int8 j = size - 1; j >= 0; --j)
                {
                    vec_res[i] += (vec_src[prev_offset + (size - 1 - j)] << (8 * j));
                }
                prev_offset = offsets_src[i];
            }
        }
        else if constexpr (std::is_same_v<ColumnFixedString, T>)
        {
            size_t n = col->getN();
            size_t prev_offset = 0;

            for (UInt32 & vec_re : vec_res)
            {
                size_t size = UTF8::seqLength(vec_src[prev_offset]);
                vec_re = 0;
                for (Int8 j = size - 1; j >= 0; --j)
                {
                    vec_re += (vec_src[prev_offset + (size - 1 - j)] << (8 * j));
                }
                prev_offset += n;
            }
        }

        return col_res;
    }
};

template <typename Impl, typename Name>
class FunctionsCharCoding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsCharCoding>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
        {
            throw Exception(
                "Number of arguments for function " + getName() + " can't be " + toString(arguments.size()) + ", should be at least 1",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isStringOrFixedString(arguments[0]) && !isNumber(arguments[0]))
        {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if constexpr (std::is_same_v<Impl, AsciiImpl>)
            return std::make_shared<DataTypeUInt8>();

        return std::make_shared<DataTypeUInt32>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            return Impl::template execute<ColumnString>(col);
        }
        else if (const ColumnFixedString * col_fixed_str = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            return Impl::template execute<ColumnFixedString>(col_fixed_str);
        }
        #define D(TYPE) \
            else if (checkColumn<ColumnDecimal<TYPE>>(column.get())) \
            { \
                const ColumnDecimal<TYPE> * col_decimal = checkAndGetColumn<ColumnDecimal<TYPE>>(column.get()); \
                if (std::is_same_v<Impl, AsciiImpl>) \
                { \
                    return executeNumbers<UInt8, ColumnUInt8, ColumnDecimal<TYPE>>(col_decimal); \
                } \
                else \
                { \
                    return executeNumbers<UInt32, ColumnUInt32, ColumnDecimal<TYPE>>(col_decimal); \
                } \
            }
                APPLY_FOR_EACH_DECIMAL_TYPE(D)
        #undef D
        #define V(TYPE) \
            else if (checkColumn<ColumnVector<TYPE>>(column.get())) \
            { \
                const ColumnVector<TYPE> * col_numeric = checkAndGetColumn<ColumnVector<TYPE>>(column.get()); \
                if (std::is_same_v<Impl, AsciiImpl>) \
                { \
                    return executeNumbers<UInt8, ColumnUInt8, ColumnVector<TYPE>>(col_numeric); \
                } \
                else \
                { \
                    return executeNumbers<UInt32, ColumnUInt32, ColumnVector<TYPE>>(col_numeric); \
                } \
            }
                APPLY_FOR_EACH_NUMERIC_TYPE(V)
        #undef V
        else throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename P, typename CONTAINER_T, typename T>
    /// templated function to handle ColumnVector<TYPE> or ColumnDecimal<TYPE> as input,
    /// and ColumnUInt8 or ColumnUInt32 as return type, 
    ColumnPtr executeNumbers(const T * col_src) const
    {
        using T2 = typename T::ValueType;
        auto col_res = CONTAINER_T::create();
        auto & vec_res = col_res->getData();
        vec_res.resize(col_src->size());
        const auto & vec_src = col_src->getData();
        if constexpr (IsDecimalNumber<T2>)
        {
            for (size_t i = 0; i < col_src->size(); ++i)
            {
                vec_res[i] = static_cast<P>(toString(vec_src[i].value)[0]);
            }
        }
        else
        {
            for (size_t i = 0; i < col_src->size(); ++i)
            {
                vec_res[i] = static_cast<P>(toString(vec_src[i])[0]);
            }
        }
        return col_res;
    }
};
    struct NameAscii
    {
        static constexpr auto name = "ascii";
    };

    struct NameOrd
    {
        static constexpr auto name = "ord";
    };

    using FunctionAscii = FunctionsCharCoding<AsciiImpl, NameAscii>;
    using FunctionOrd = FunctionsCharCoding<OrdImpl, NameOrd>;

    void registerFunctionCharCoding(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionAscii>(FunctionFactory::CaseInsensitive);
        factory.registerFunction<FunctionOrd>(FunctionFactory::CaseInsensitive);
    }
}
