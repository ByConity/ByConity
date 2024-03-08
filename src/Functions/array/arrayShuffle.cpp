#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// randomly shuffle the elements of each array
/// shuffle([1,2,3,4]) => [1,3,2,4]
class FunctionArrayShuffle : public IFunction
{
public:
    static constexpr auto name = "arrayShuffle";
    static FunctionPtr create(ContextPtr ) { return std::make_shared<FunctionArrayShuffle>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override;

private:
    template <typename T>
    static bool executeNumber(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, std::mt19937 & gen);

    static bool executeFixedString(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, std::mt19937 & gen);
    static bool executeString(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, std::mt19937 & gen);
    static bool executeGeneric(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, std::mt19937 & gen);
};


static std::vector<size_t> genSequence(size_t n, std::mt19937 & gen)
{
    std::vector<size_t> sequence(n);
    for (size_t j = 0; j < n; j++)
        sequence[j] = j;
    std::shuffle(sequence.begin(), sequence.end(), gen);
    return sequence;
}


ColumnPtr FunctionArrayShuffle::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!array)
        throw Exception("Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);

    auto res_ptr = array->cloneEmpty();
    ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);
    res.getOffsetsPtr() = array->getOffsetsPtr();

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();

    IColumn & res_data = res.getData();

    const ColumnNullable * src_nullable_col = typeid_cast<const ColumnNullable *>(&src_data);
    ColumnNullable * res_nullable_col = typeid_cast<ColumnNullable *>(&res_data);

    const IColumn * src_inner_col = src_nullable_col ? &src_nullable_col->getNestedColumn() : &src_data;
    IColumn * res_inner_col = res_nullable_col ? &res_nullable_col->getNestedColumn() : &res_data;

    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen{seed};

    false // NOLINT
        || executeNumber<UInt8>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<UInt16>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<UInt32>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<UInt64>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Int8>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Int16>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Int32>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Int64>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Float32>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeNumber<Float64>(*src_inner_col, offsets, *res_inner_col, gen)
        || executeString(*src_inner_col, offsets, *res_inner_col, gen)
        || executeFixedString(*src_inner_col, offsets, *res_inner_col, gen)
        || executeGeneric(*src_inner_col, offsets, *res_inner_col, gen);

    // res_data is an empty clone of src_data
    // typeid_cast on both should behave similarly
    chassert(bool(src_nullable_col) == bool(res_nullable_col));

    if (src_nullable_col)
    {
        /// reset the generator so that the null map will get the same sequence
        gen.seed(seed);
        if (!executeNumber<UInt8>(src_nullable_col->getNullMapColumn(), offsets, res_nullable_col->getNullMapColumn(), gen))
            throw Exception("Illegal column " + src_nullable_col->getNullMapColumn().getName()
                    + " of null map of the first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
    }

    return res_ptr;
}


bool FunctionArrayShuffle::executeGeneric(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, std::mt19937 & gen)
{
    size_t size = src_array_offsets.size();
    res_data.reserve(size);

    ColumnArray::Offset src_prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        size_t array_len = src_array_offsets[i] - src_prev_offset;
        const auto sequence = genSequence(array_len, gen);
        for (size_t j = 0; j < array_len; j++)
            res_data.insertFrom(src_data, sequence[j] + src_prev_offset);

        src_prev_offset = src_array_offsets[i];
    }

    return true;
}

template <typename T>
bool FunctionArrayShuffle::executeNumber(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, std::mt19937 & gen)
{
    if (const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data))
    {
        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();
        PaddedPODArray<T> & res_vec = typeid_cast<ColumnVector<T> &>(res_data).getData();
        res_vec.resize(src_data.size());

        size_t size = src_offsets.size();
        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const auto * src = &src_vec[src_prev_offset];
            auto * dst = &res_vec[src_prev_offset];

            size_t array_len = src_offsets[i] - src_prev_offset;
            const auto sequence = genSequence(array_len, gen);

            for (size_t j = 0; j < array_len; j++)
                *(dst + j) = *(src + sequence[j]);

            src_prev_offset = src_offsets[i];
        }

        return true;
    }
    else
        return false;
}

bool FunctionArrayShuffle::executeFixedString(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, IColumn & res_data, std::mt19937 & gen)
{
    if (const ColumnFixedString * src_data_concrete = checkAndGetColumn<ColumnFixedString>(&src_data))
    {
        const size_t n = src_data_concrete->getN();
        const ColumnFixedString::Chars & src_data_chars = src_data_concrete->getChars();
        ColumnFixedString::Chars & res_chars = typeid_cast<ColumnFixedString &>(res_data).getChars();
        size_t size = src_offsets.size();
        res_chars.resize(src_data_chars.size());

        ColumnArray::Offset src_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const UInt8 * src = &src_data_chars[src_prev_offset * n];
            UInt8 * dst = &res_chars[src_prev_offset * n];

            size_t array_len = src_offsets[i] - src_prev_offset;
            const auto sequence = genSequence(array_len, gen);
            for (size_t j = 0; j < array_len; j++)
                /// NOTE: memcpySmallAllowReadWriteOverflow15 doesn't work correctly here.
                memcpy(&dst[j * n], &src[sequence[j] * n], n);

            src_prev_offset = src_offsets[i];
        }
        return true;
    }
    else
        return false;
}

bool FunctionArrayShuffle::executeString(const IColumn & src_data, const ColumnArray::Offsets & src_array_offsets, IColumn & res_data, std::mt19937 & gen)
{
    if (const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data))
    {
        const ColumnString::Offsets & src_string_offsets = src_data_concrete->getOffsets();
        ColumnString::Offsets & res_string_offsets = typeid_cast<ColumnString &>(res_data).getOffsets();

        const ColumnString::Chars & src_data_chars = src_data_concrete->getChars();
        ColumnString::Chars & res_chars = typeid_cast<ColumnString &>(res_data).getChars();

        size_t size = src_array_offsets.size();
        res_string_offsets.resize(src_string_offsets.size());
        res_chars.resize(src_data_chars.size());

        ColumnArray::Offset src_array_prev_offset = 0;
        ColumnString::Offset res_string_prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            if (src_array_offsets[i] != src_array_prev_offset)
            {
                size_t array_size = src_array_offsets[i] - src_array_prev_offset;
                const auto sequence = genSequence(array_size, gen);
                for (size_t j = 0; j < array_size; ++j)
                {
                    auto src_pos = src_string_offsets[src_array_prev_offset + sequence[j] - 1];
                    size_t string_size = src_string_offsets[src_array_prev_offset + sequence[j]] - src_pos;

                    memcpySmallAllowReadWriteOverflow15(&res_chars[res_string_prev_offset], &src_data_chars[src_pos], string_size);

                    res_string_prev_offset += string_size;
                    res_string_offsets[src_array_prev_offset + j] = res_string_prev_offset;
                }
            }

            src_array_prev_offset = src_array_offsets[i];
        }

        return true;
    }
    else
        return false;
}


REGISTER_FUNCTION(ArrayShuffle)
{
    factory.registerFunction<FunctionArrayShuffle>();
    factory.registerAlias("shuffle", FunctionArrayShuffle::name, FunctionFactory::CaseInsensitive);
}

}
