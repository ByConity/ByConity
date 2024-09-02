/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/array/arrayElement.h>
#include <Functions/castTypeToEither.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/MapHelpers.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace ArrayImpl
{

/// under MySQL, element_at always return nullable result.
/// NullMapBuilder is used to mark NULL source elements and result/sink elements
class NullMapBuilder
{
public:
    explicit operator bool() const { return is_mysql || src_null_map; }
    bool operator!() const { return !(is_mysql || src_null_map); }

    void initSource(const UInt8 * src_null_map_, bool is_mysql_)
    {
        src_null_map = src_null_map_;
        is_mysql = is_mysql_;
    }

    /// pass the index null map to check if the index is 0 or null.
    /// if the index is null, the result is null
    /// if the index is 0, throw error
    void initSink(size_t size, ColumnPtr index_null_map)
    {
        auto sink = ColumnUInt8::create(0);
        if (index_null_map)
        {
            auto index_full_col = index_null_map->convertToFullColumnIfConst();
            assert(size == index_full_col->size());
            sink->assumeMutable()->insertRangeFrom(*index_full_col, 0, size);
        }
        else
        {
            sink->assumeMutable()->insertManyDefaults(size);
        }
        sink_null_map = sink->getData().data();
        sink_null_map_holder = std::move(sink);
    }

    void update(size_t from)
    {
        sink_null_map[index] |= bool(src_null_map && src_null_map[from]);
        ++index;
    }

    void update()
    {
        sink_null_map[index] |= is_mysql | bool(src_null_map);
        ++index;
    }

    ColumnPtr getNullMapColumnPtr() && { return std::move(sink_null_map_holder); }

    bool notNullUnderMySql(size_t i) { return !sink_null_map[i] && is_mysql; }

private:
    const UInt8 * src_null_map = nullptr;
    UInt8 * sink_null_map = nullptr;
    MutableColumnPtr sink_null_map_holder;
    size_t index = 0;
    bool is_mysql = false;
};

}

namespace
{

template <typename T>
struct ArrayElementNumImpl
{
    /** Implementation for constant index.
      * If negative = false - index is from beginning of array, started from 0.
      * If negative = true - index is from end of array, started from 0.
      */
    template <bool negative>
    static void vectorConst(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index, const DataTypePtr value_type,
        PaddedPODArray<T> & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = !negative ? (current_offset + index) : (offsets[i] - index - 1);
                result[i] = data[j];
                if (builder)
                    builder.update(j);
            }
            else
            {
                result[i] = value_type->getDefault().get<T>();

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices, const DataTypePtr value_type,
        PaddedPODArray<T> & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result[i] = data[j];

                if (builder)
                    builder.update(j);
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result[i] = data[j];

                if (builder)
                    builder.update(j);
            }
            else if (index == 0 && builder && builder.notNullUnderMySql(i))
            {
                throw Exception("Array indices are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
            {
                result[i] = value_type->getDefault().get<T>();

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

struct ArrayElementStringImpl
{
    template <bool negative>
    static void vectorConst(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const ColumnArray::Offset index,
        ColumnString::Chars & result_data, ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);
        result_data.reserve(data.size());

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_result_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t adjusted_index = !negative ? index : (array_size - index - 1);

                size_t j = current_offset + adjusted_index;
                if (builder)
                    builder.update(j);

                ColumnArray::Offset string_pos = current_offset == 0 && adjusted_index == 0
                    ? 0
                    : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;

                result_data.resize(current_result_offset + string_size);
                memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], &data[string_pos], string_size);
                current_result_offset += string_size;
                result_offsets[i] = current_result_offset;
            }
            else
            {
                /// Insert an empty row.
                result_data.resize(current_result_offset + 1);
                result_data[current_result_offset] = 0;
                current_result_offset += 1;
                result_offsets[i] = current_result_offset;

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets, const ColumnString::Offsets & string_offsets,
        const PaddedPODArray<TIndex> & indices,
        ColumnString::Chars & result_data, ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);
        result_data.reserve(data.size());

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_result_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            size_t adjusted_index;    /// index in array from zero

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
                adjusted_index = index - 1;
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
                adjusted_index = array_size + index;
            else if (index == 0 && builder && builder.notNullUnderMySql(i))
                throw Exception("Array indices are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            else
                adjusted_index = array_size;    /// means no element should be taken

            if (adjusted_index < array_size)
            {
                size_t j = current_offset + adjusted_index;
                if (builder)
                    builder.update(j);

                ColumnArray::Offset string_pos = current_offset == 0 && adjusted_index == 0
                    ? 0
                    : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;

                result_data.resize(current_result_offset + string_size);
                memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], &data[string_pos], string_size);
                current_result_offset += string_size;
                result_offsets[i] = current_result_offset;
            }
            else
            {
                /// Insert empty string
                result_data.resize(current_result_offset + 1);
                result_data[current_result_offset] = 0;
                current_result_offset += 1;
                result_offsets[i] = current_result_offset;

                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

/// Generic implementation for other nested types.
struct ArrayElementGenericImpl
{
    template <bool negative>
    static void vectorConst(
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index,
        IColumn & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = !negative ? current_offset + index : offsets[i] - index - 1;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else
            {
                result.insertDefault();
                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const IColumn & data, const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices,
        IColumn & result, ArrayImpl::NullMapBuilder & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result.insertFrom(data, j);
                if (builder)
                    builder.update(j);
            }
            else if (index == 0 && builder && builder.notNullUnderMySql(i))
            {
                throw Exception("Array indices are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
            {
                result.insertDefault();
                if (builder)
                    builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

}


FunctionPtr FunctionArrayElement::create(ContextPtr context)
{
    return std::make_shared<FunctionArrayElement>(context);
}


template <typename DataType>
ColumnPtr FunctionArrayElement::executeNumberConst(
    const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
    if (!array_type)
        return nullptr;

    auto col_res = ColumnVector<DataType>::create();

    if (index.getType() == Field::Types::UInt64)
    {
        ArrayElementNumImpl<DataType>::template vectorConst<false>(
            col_nested->getData(), col_array->getOffsets(), safeGet<UInt64>(index) - 1, array_type->getNestedType(), col_res->getData(), builder);
    }
    else if (index.getType() == Field::Types::Int64)
    {
        /// Cast to UInt64 before negation allows to avoid undefined behaviour for negation of the most negative number.
        /// NOTE: this would be undefined behaviour in C++ sense, but nevertheless, compiler cannot see it on user provided data,
        /// and generates the code that we want on supported CPU architectures (overflow in sense of two's complement arithmetic).
        /// This is only needed to avoid UBSan report.

        /// Negative array indices work this way:
        /// arr[-1] is the element at offset 0 from the last
        /// arr[-2] is the element at offset 1 from the last and so on.

        ArrayElementNumImpl<DataType>::template vectorConst<true>(
            col_nested->getData(), col_array->getOffsets(), -(UInt64(safeGet<Int64>(index)) + 1), array_type->getNestedType(), col_res->getData(), builder);
    }
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    return col_res;
}

template <typename IndexType, typename DataType>
ColumnPtr FunctionArrayElement::executeNumber(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnVector<DataType> * col_nested = checkAndGetColumn<ColumnVector<DataType>>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
    if (!array_type)
        return nullptr;

    auto col_res = ColumnVector<DataType>::create();

    ArrayElementNumImpl<DataType>::template vector<IndexType>(
        col_nested->getData(), col_array->getOffsets(), indices, array_type->getNestedType(), col_res->getData(), builder);

    return col_res;
}

ColumnPtr
FunctionArrayElement::executeStringConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    auto col_res = ColumnString::create();

    if (index.getType() == Field::Types::UInt64)
        ArrayElementStringImpl::vectorConst<false>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            safeGet<UInt64>(index) - 1,
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementStringImpl::vectorConst<true>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            -(UInt64(safeGet<Int64>(index)) + 1),
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    return col_res;
}

template <typename IndexType>
ColumnPtr FunctionArrayElement::executeString(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    auto col_res = ColumnString::create();

    ArrayElementStringImpl::vector<IndexType>(
        col_nested->getChars(),
        col_array->getOffsets(),
        col_nested->getOffsets(),
        indices,
        col_res->getChars(),
        col_res->getOffsets(),
        builder);

    return col_res;
}

ColumnPtr FunctionArrayElement::executeGenericConst(
    const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    if (index.getType() == Field::Types::UInt64)
        ArrayElementGenericImpl::vectorConst<false>(
            col_nested, col_array->getOffsets(), safeGet<UInt64>(index) - 1, *col_res, builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementGenericImpl::vectorConst<true>(
            col_nested, col_array->getOffsets(), -(UInt64(safeGet<Int64>(index) + 1)), *col_res, builder);
    else
        throw Exception("Illegal type of array index", ErrorCodes::LOGICAL_ERROR);

    return col_res;
}

template <typename IndexType>
ColumnPtr FunctionArrayElement::executeGeneric(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    ArrayElementGenericImpl::vector<IndexType>(
        col_nested, col_array->getOffsets(), indices, *col_res, builder);

    return col_res;
}

template <typename IndexType>
ColumnPtr FunctionArrayElement::executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                        const PaddedPODArray <IndexType> & indices, ArrayImpl::NullMapBuilder & builder,
                                        size_t input_rows_count)
{
    const ColumnArray * col_array = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    auto res = result_type->createColumn();

    size_t rows = input_rows_count;
    const IColumn & array_elements = col_array->getData();
    size_t array_size = array_elements.size();

    for (size_t i = 0; i < rows; ++i)
    {
        IndexType index = indices[i];
        if (index > 0 && static_cast<size_t>(index) <= array_size)
        {
            size_t j = index - 1;
            res->insertFrom(array_elements, j);
            if (builder)
                builder.update(j);
        }
        else if (index < 0 && -static_cast<size_t>(index) <= array_size)
        {
            size_t j = array_size + index;
            res->insertFrom(array_elements, j);
            if (builder)
                builder.update(j);
        }
        else
        {
            res->insertDefault();
            if (builder)
                builder.update();
        }
    }

    return res;
}

template <typename IndexType>
ColumnPtr FunctionArrayElement::executeArgument(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const
{
    auto index = checkAndGetColumn<ColumnVector<IndexType>>(arguments[1].column.get());
    if (!index)
        return nullptr;
    const auto & index_data = index->getData();

    ColumnPtr res;
    if (!((res = executeNumber<IndexType, UInt8>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, UInt16>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, UInt32>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, UInt64>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Int8>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Int16>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Int32>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Int64>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Float32>(arguments, index_data, builder))
        || (res = executeNumber<IndexType, Float64>(arguments, index_data, builder))
        || (res = executeConst<IndexType>(arguments, result_type, index_data, builder, input_rows_count))
        || (res = executeString<IndexType>(arguments, index_data, builder))
        || (res = executeGeneric<IndexType>(arguments, index_data, builder))))
    throw Exception("Illegal column " + arguments[0].column->getName()
                + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

    return res;
}

ColumnPtr FunctionArrayElement::executeTuple(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnTuple * col_nested = typeid_cast<const ColumnTuple *>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    if (is_mysql_dialect)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrayElement(col, idx) where col is Array(Tuple(T)) is not supported under MySQL.");

    const auto & tuple_columns = col_nested->getColumns();
    size_t tuple_size = tuple_columns.size();

    const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(
        *typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType()).getElements();

    /** We will calculate the function for the tuple of the internals of the array.
      * To do this, create a temporary columns.
      * It will consist of the following columns
      * - the index of the array to be taken;
      * - an array of the first elements of the tuples;
      * - the result of taking the elements by the index for an array of the first elements of the tuples;
      * - array of the second elements of the tuples;
      * - result of taking elements by index for an array of second elements of tuples;
      * ...
      */
    ColumnsWithTypeAndName temporary_results(2);
    temporary_results[1] = arguments[1];

    /// results of taking elements by index for arrays from each element of the tuples;
    Columns result_tuple_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
    {
        ColumnWithTypeAndName array_of_tuple_section;
        array_of_tuple_section.column = ColumnArray::create(tuple_columns[i], col_array->getOffsetsPtr());
        array_of_tuple_section.type = std::make_shared<DataTypeArray>(tuple_types[i]);
        temporary_results[0] = array_of_tuple_section;

        auto type = getReturnTypeImpl(temporary_results);
        auto col = executeImpl(temporary_results, type, input_rows_count);
        result_tuple_columns[i] = std::move(col);
    }

    return ColumnTuple::create(result_tuple_columns);
}

namespace
{

template<typename DataColumn, typename IndexColumn>
struct MatcherString
{
    const DataColumn & data;
    const IndexColumn & index;

    bool match(size_t row_data, size_t row_index) const
    {
        auto data_ref = data.getDataAt(row_data);
        auto index_ref = index.getDataAt(row_index);
        return memequalSmallAllowOverflow15(index_ref.data, index_ref.size, data_ref.data, data_ref.size);
    }
};

template<typename DataColumn>
struct MatcherStringConst
{
    const DataColumn & data;
    const String & index;

    bool match(size_t row_data, size_t /* row_index */) const
    {
        auto data_ref = data.getDataAt(row_data);
        return index.size() == data_ref.size && memcmp(index.data(), data_ref.data, data_ref.size) == 0;
    }
};

template <typename DataType, typename IndexType>
struct MatcherNumber
{
    const PaddedPODArray<DataType> & data;
    const PaddedPODArray<IndexType> & index;

    bool match(size_t row_data, size_t row_index) const
    {
        return data[row_data] == static_cast<DataType>(index[row_index]);
    }
};

template <typename DataType>
struct MatcherNumberConst
{
    const PaddedPODArray<DataType> & data;
    DataType index;

    bool match(size_t row_data, size_t /* row_index */) const
    {
        return data[row_data] == index;
    }
};

}

template <typename Matcher>
void FunctionArrayElement::executeMatchKeyToIndex(
    const Offsets & offsets, PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher)
{
    size_t rows = offsets.size();
    for (size_t i = 0; i < rows; ++i)
    {
        bool matched = false;
        size_t begin = offsets[i - 1];
        size_t end = offsets[i];
        for (size_t j = begin; j < end; ++j)
        {
            if (matcher.match(j, i))
            {
                matched_idxs.push_back(j - begin + 1);
                matched = true;
                break;
            }
        }

        if (!matched)
            matched_idxs.push_back(0);
    }
}

template <typename Matcher>
void FunctionArrayElement::executeMatchConstKeyToIndex(
    size_t num_rows, size_t num_values,
    PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        bool matched = false;
        for (size_t j = 0; j < num_values; ++j)
        {
            if (matcher.match(j, i))
            {
                matched_idxs.push_back(j + 1);
                matched = true;
                break;
            }
        }

        if (!matched)
            matched_idxs.push_back(0);
    }
}

template <typename F>
static bool castColumnString(const IColumn * column, F && f)
{
    return castTypeToEither<ColumnString, ColumnFixedString>(column, std::forward<F>(f));
}

bool FunctionArrayElement::matchKeyToIndexStringConst(
    const IColumn & data, const Offsets & offsets,
    const Field & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnString(&data, [&](const auto & data_column)
    {
        using DataColumn = std::decay_t<decltype(data_column)>;

        MatcherStringConst<DataColumn> matcher{data_column, get<const String &>(index)};
        executeMatchKeyToIndex(offsets, matched_idxs, matcher);
        return true;
    });
}

bool FunctionArrayElement::matchKeyToIndexString(
    const IColumn & data, const Offsets & offsets, bool is_key_const,
    const IColumn & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnString(&data, [&](const auto & data_column)
    {
        return castColumnString(&index, [&](const auto & index_column)
        {
            using DataColumn = std::decay_t<decltype(data_column)>;
            using IndexColumn = std::decay_t<decltype(index_column)>;

            MatcherString<DataColumn, IndexColumn> matcher{data_column, index_column};
            if (is_key_const)
                executeMatchConstKeyToIndex(index.size(), data.size(), matched_idxs, matcher);
            else
                executeMatchKeyToIndex(offsets, matched_idxs, matcher);

            return true;
        });
    });
}

template <typename F>
static bool castColumnNumeric(const IColumn * column, F && f)
{
    return castTypeToEither<
        ColumnVector<UInt8>,
        ColumnVector<UInt16>,
        ColumnVector<UInt32>,
        ColumnVector<UInt64>,
        ColumnVector<UInt128>,
        ColumnVector<UInt256>,
        ColumnVector<Int8>,
        ColumnVector<Int16>,
        ColumnVector<Int32>,
        ColumnVector<Int64>,
        ColumnVector<Int128>,
        ColumnVector<Int256>,
        ColumnVector<UUID>,
        ColumnVector<IPv4>,
        ColumnVector<IPv6>,
        ColumnVector<Float32>,
        ColumnVector<Float64>
    >(column, std::forward<F>(f));
}

bool FunctionArrayElement::matchKeyToIndexNumberConst(
    const IColumn & data, const Offsets & offsets,
    const Field & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnNumeric(&data, [&](const auto & data_column)
    {
        using DataType = typename std::decay_t<decltype(data_column)>::ValueType;
        std::optional<DataType> index_as_number;

        Field::dispatch([&](const auto & value)
        {
            using FieldType = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<FieldType, DataType>
                || ((is_integer_v<FieldType> || std::is_floating_point_v<FieldType>) && std::is_convertible_v<FieldType, DataType>))
                index_as_number = static_cast<DataType>(value);
        }, index);

        if (!index_as_number)
            return false;

        MatcherNumberConst<DataType> matcher{data_column.getData(), *index_as_number};
        executeMatchKeyToIndex(offsets, matched_idxs, matcher);
        return true;
    });
}

bool FunctionArrayElement::matchKeyToIndexNumber(
    const IColumn & data, const Offsets & offsets, bool is_key_const,
    const IColumn & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnNumeric(&data, [&](const auto & data_column)
    {
        return castColumnNumeric(&index, [&](const auto & index_column)
        {
            using DataType = typename std::decay_t<decltype(data_column)>::ValueType;
            using IndexType = typename std::decay_t<decltype(index_column)>::ValueType;

            if constexpr (std::is_same_v<IndexType, DataType>
                || ((is_integer_v<IndexType> || std::is_floating_point_v<IndexType>) && std::is_convertible_v<IndexType, DataType>))
            {
                MatcherNumber<DataType, IndexType> matcher{data_column.getData(), index_column.getData()};
                if (is_key_const)
                    executeMatchConstKeyToIndex(index_column.size(), data_column.size(), matched_idxs, matcher);
                else
                    executeMatchKeyToIndex(offsets, matched_idxs, matcher);

                return true;
            }

            return false;
        });
    });
}

ColumnPtr FunctionArrayElement::executeMap(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, ColumnPtr index_null_map_col) const
{
    const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
    const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
    const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
    if ((!col_map && !col_const_map) || !map_type)
        return nullptr;

    if (col_const_map)
        col_map = typeid_cast<const ColumnMap *>(&col_const_map->getDataColumn());

    const auto & nested_column = col_map->getNestedColumn();
    const auto & keys_data = col_map->getNestedData().getColumn(0);
    const auto & values_data = col_map->getNestedData().getColumn(1);
    const auto & offsets = nested_column.getOffsets();

    /// At first step calculate indices in array of values for requested keys.
    auto indices_column = DataTypeNumber<UInt64>().createColumn();
    indices_column->reserve(input_rows_count);
    auto & indices_data = assert_cast<ColumnVector<UInt64> &>(*indices_column).getData();

    bool executed = false;
    if (!isColumnConst(*arguments[1].column))
    {
        executed = matchKeyToIndexNumber(keys_data, offsets, !!col_const_map, *arguments[1].column, indices_data)
            || matchKeyToIndexString(keys_data, offsets, !!col_const_map, *arguments[1].column, indices_data);
    }
    else
    {
        Field index = (*arguments[1].column)[0];

        /// try convert const index to right type
        auto index_lit = std::make_shared<ASTLiteral>(index);
        Field key_field = tryConvertToMapKeyField(map_type->getKeyType(), index_lit->getColumnName());
        if (!key_field.isNull())
            index = key_field;

        executed = matchKeyToIndexNumberConst(keys_data, offsets, index, indices_data)
            || matchKeyToIndexStringConst(keys_data, offsets, index, indices_data);
    }

    if (!executed)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types of arguments: {}, {} for function {}",
            arguments[0].type->getName(), arguments[1].type->getName(), getName());

    ColumnPtr values_array_nested = values_data.getPtr();
    DataTypePtr result_value_type = map_type->getValueType();

    // TODO(shiyuze): Map can be nested in Map but cannot wrapped in Nullable
    if (is_mysql_dialect && result_type->isNullable())
    {
        /// In MYSQL dialect type, non-exists indices will return Null, so we need to wrap
        /// new arguments in nullable
        values_array_nested = makeNullable(values_array_nested);
        result_value_type = makeNullable(result_value_type);
    }

    ColumnPtr values_array = ColumnArray::create(values_array_nested, nested_column.getOffsetsPtr());
    if (col_const_map)
        values_array = ColumnConst::create(values_array, input_rows_count);

    /// Prepare arguments to call arrayElement for array with values and calculated indices at previous step.
    ColumnsWithTypeAndName new_arguments =
    {
        {
            values_array,
            std::make_shared<DataTypeArray>(result_value_type),
            ""
        },
        {
            std::move(indices_column),
            std::make_shared<DataTypeNumber<UInt64>>(),
            ""
        }
    };

    /// In MYSQL dialect type, we also need to set is_mysql to false to get Null result of all non-exists
    /// indices. Indices returned by matchKeyToIndexXXX using 0 for mismatched, after we set is_mysql to false
    /// and wrap array nested column in nullable, so all the indices with value 0 will return default value
    /// of Nullable (which is Null).
    auto res = executeImpl(new_arguments, result_type, input_rows_count, false);

    /// If index is nullable, we need to update its null map to result column
    if (is_mysql_dialect && result_type->isNullable() && index_null_map_col)
    {
        UInt8 * res_null_map_data = typeid_cast<ColumnNullable &>(*(res->assumeMutable())).getNullMapData().data();
        const UInt8 * index_null_map_data = typeid_cast<const ColumnUInt8 &>(*index_null_map_col).getData().data();

        for (size_t i = 0; i < index_null_map_col->size(); i++)
            res_null_map_data[i] |= index_null_map_data[i];
    }

    return res;
}

String FunctionArrayElement::getName() const
{
    return name;
}

DataTypePtr FunctionArrayElement::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    NullPresence null_presence = getNullPresense(arguments);
    if (null_presence.has_null_constant)
        return makeNullable(std::make_shared<DataTypeNothing>());

    auto arg0_no_null = arguments[0].type;
    if (is_mysql_dialect)
        arg0_no_null = removeNullable(arguments[0].type);

    if (const auto * map_type = checkAndGetDataType<DataTypeMap>(arg0_no_null.get()))
        return is_mysql_dialect ? makeNullable(map_type->getValueType()) : map_type->getValueType();

    const auto * array_type = checkAndGetDataType<DataTypeArray>(arg0_no_null.get());
    if (!array_type)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function '{}' must be array, got '{}' instead",
            getName(), arguments[0].type->getName());
    }

    auto arg1_type = arguments[1].type;
    if (!(isInteger(arg1_type) || (is_mysql_dialect && (isInteger(removeNullable(arg1_type)) || arg1_type->onlyNull()))))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function '{}' must be integer, got '{}' instead",
            getName(), arg1_type->getName());
    }

    auto ret_type = array_type->getNestedType();
    if (is_mysql_dialect)
        ret_type = makeNullable(ret_type);
    return ret_type;
}

ColumnPtr FunctionArrayElement::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    return executeImpl(arguments, result_type, input_rows_count, is_mysql_dialect);
}

ColumnPtr FunctionArrayElement::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool is_mysql) const
{
    ColumnsWithTypeAndName tmp_args = arguments;
    DataTypePtr tmp_result_type = result_type;

    ColumnPtr index_null_map_col = nullptr;
    NullPresence null_presence;
    if (is_mysql)
    {
        /// ref IExecutableFunction::defaultImplementationForNulls
        /// additionally, we extract the index null map if exists
        null_presence = getNullPresense(arguments);
        if (null_presence.has_nullable)
        {
            /// if index is const null -> return directly
            if (null_presence.has_null_constant)
            {
                // Default implementation for nulls returns null result for null arguments,
                // so the result type must be nullable.
                if (!result_type->isNullable())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Function {} with Null argument and default implementation for Nulls "
                            "is expected to return Nullable result, got {}", getName(), result_type->getName());

                return result_type->createColumnConstWithDefaultValue(input_rows_count);
            }

            tmp_args = createBlockWithNestedColumns(arguments);
            tmp_result_type = removeNullable(result_type);

            /// if index is const but not null, the index_null_map_col would be all 0s, no need to get it.
            /// if index is nullable column -> get the null map to apply to the result
            if (const auto * index_null_col = checkAndGetColumn<ColumnNullable>(*arguments[1].column); index_null_col)
                index_null_map_col = index_null_col->getNullMapColumnPtr();
        }
    }

    const auto * col_map = checkAndGetColumn<ColumnMap>(tmp_args[0].column.get());
    const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(tmp_args[0].column.get());

    if (col_map || col_const_map)
    {
        /// In MYSQL dialect, arrayElement will always return Nullable(ValueType) is args0 is Map,
        /// so we need to use result_type here.
        return executeMap(tmp_args, result_type, input_rows_count, index_null_map_col);
    }

    /// Check nullability.
    bool is_array_of_nullable = false;
    const ColumnArray * col_array = nullptr;
    const ColumnArray * col_const_array = nullptr;

    col_array = checkAndGetColumn<ColumnArray>(tmp_args[0].column.get());
    if (col_array)
    {
        is_array_of_nullable = isColumnNullable(col_array->getData());
    }
    else
    {
        col_const_array = checkAndGetColumnConstData<ColumnArray>(tmp_args[0].column.get());
        if (col_const_array)
            is_array_of_nullable = isColumnNullable(col_const_array->getData());
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    if (!is_array_of_nullable && !is_mysql)
    {
        ArrayImpl::NullMapBuilder builder;
        return perform(arguments, result_type, builder, input_rows_count);
    }
    else if (is_array_of_nullable)
    {
        /// Perform initializations.
        ArrayImpl::NullMapBuilder builder;
        ColumnsWithTypeAndName source_columns;

        const DataTypePtr & input_type = typeid_cast<const DataTypeNullable &>(
            *typeid_cast<const DataTypeArray &>(*tmp_args[0].type).getNestedType()).getNestedType();

        DataTypePtr tmp_ret_type = removeNullable(tmp_result_type);

        if (col_array)
        {
            const auto & nullable_col = typeid_cast<const ColumnNullable &>(col_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            /// Put nested_col inside a ColumnArray.
            source_columns =
            {
                {
                    ColumnArray::create(nested_col, col_array->getOffsetsPtr()),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                tmp_args[1],
            };

            builder.initSource(nullable_col.getNullMapData().data(), is_mysql);
        }
        else
        {
            /// ColumnConst(ColumnArray(ColumnNullable(...)))
            const auto & nullable_col = assert_cast<const ColumnNullable &>(col_const_array->getData());
            const auto & nested_col = nullable_col.getNestedColumnPtr();

            source_columns =
            {
                {
                    ColumnConst::create(ColumnArray::create(nested_col, col_const_array->getOffsetsPtr()), input_rows_count),
                    std::make_shared<DataTypeArray>(input_type),
                    ""
                },
                tmp_args[1],
            };

            builder.initSource(nullable_col.getNullMapData().data(), is_mysql);
        }

        builder.initSink(input_rows_count, index_null_map_col);

        auto res = perform(source_columns, tmp_ret_type, builder, input_rows_count);
        res = ColumnNullable::create(res, builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());

        if (null_presence.has_nullable)
            /// apply the null map from the original arguments
            return wrapInNullable(res, arguments, result_type, input_rows_count);
        else
            return res;
    }
    else
    {
        ArrayImpl::NullMapBuilder builder;
        builder.initSource(nullptr, is_mysql);
        builder.initSink(input_rows_count, index_null_map_col);

        auto res = perform(tmp_args, tmp_result_type, builder, input_rows_count);
        res = ColumnNullable::create(res, builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());

        if (null_presence.has_nullable)
            /// apply the null map from the original arguments
            return wrapInNullable(res, arguments, result_type, input_rows_count);
        else
            return res;
    }
}

ColumnPtr FunctionArrayElement::perform(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                   ArrayImpl::NullMapBuilder & builder, size_t input_rows_count) const
{
    ColumnPtr res;
    if ((res = executeTuple(arguments, input_rows_count)))
        return res;
    else if (!isColumnConst(*arguments[1].column))
    {
        if (!((res = executeArgument<UInt8>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<UInt16>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<UInt32>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<UInt64>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<Int8>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<Int16>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<Int32>(arguments, result_type, builder, input_rows_count))
            || (res = executeArgument<Int64>(arguments, result_type, builder, input_rows_count))))
        throw Exception("Second argument for function " + getName() + " must have UInt or Int type.",
                        ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        Field index = (*arguments[1].column)[0];

        if (index == 0u)
            throw Exception("Array indices are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);

        if (!((res = executeNumberConst<UInt8>(arguments, index, builder))
            || (res = executeNumberConst<UInt16>(arguments, index, builder))
            || (res = executeNumberConst<UInt32>(arguments, index, builder))
            || (res = executeNumberConst<UInt64>(arguments, index, builder))
            || (res = executeNumberConst<Int8>(arguments, index, builder))
            || (res = executeNumberConst<Int16>(arguments, index, builder))
            || (res = executeNumberConst<Int32>(arguments, index, builder))
            || (res = executeNumberConst<Int64>(arguments, index, builder))
            || (res = executeNumberConst<Float32>(arguments, index, builder))
            || (res = executeNumberConst<Float64>(arguments, index, builder))
            || (res = executeStringConst (arguments, index, builder))
            || (res = executeGenericConst (arguments, index, builder))))
        throw Exception("Illegal column " + arguments[0].column->getName()
            + " of first argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }

    return res;
}


REGISTER_FUNCTION(ArrayElement)
{
    factory.registerFunction<FunctionArrayElement>();
    factory.registerAlias("element_at", FunctionArrayElement::name, FunctionFactory::CaseInsensitive);
}

}
