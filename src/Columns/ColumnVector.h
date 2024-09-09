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

#pragma once

#include <cmath>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnVectorHelper.h>
#include <common/unaligned.h>
#include <Core/Field.h>
#include <Common/assert_cast.h>
#include <IO/UncompressedCache.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <class T, class U = T>
struct CompareHelper
{
    static constexpr bool less(T a, U b, int /*nan_direction_hint*/) { return a < b; }
    static constexpr bool greater(T a, U b, int /*nan_direction_hint*/) { return a > b; }
    static constexpr bool equals(T a, U b, int /*nan_direction_hint*/) { return a == b; }

    /** Compares two numbers. Returns a number less than zero, equal to zero, or greater than zero if a < b, a == b, a > b, respectively.
      * If one of the values is NaN, then
      * - if nan_direction_hint == -1 - NaN are considered less than all numbers;
      * - if nan_direction_hint == 1 - NaN are considered to be larger than all numbers;
      * Essentially: nan_direction_hint == -1 says that the comparison is for sorting in descending order.
      */
    static constexpr int compare(T a, U b, int /*nan_direction_hint*/)
    {
        return a > b ? 1 : (a < b ? -1 : 0);
    }
};

template <class T>
struct FloatCompareHelper
{
    static constexpr bool less(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint < 0;
        if (isnan_b)
            return nan_direction_hint > 0;

        return a < b;
    }

    static constexpr bool greater(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint > 0;
        if (isnan_b)
            return nan_direction_hint < 0;

        return a > b;
    }

    static constexpr bool equals(T a, T b, int nan_direction_hint)
    {
        return compare(a, b, nan_direction_hint) == 0;
    }

    static constexpr int compare(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (unlikely(isnan_a || isnan_b))
        {
            if (isnan_a && isnan_b)
                return 0;

            return isnan_a
                ? nan_direction_hint
                : -nan_direction_hint;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <class U> struct CompareHelper<Float32, U> : public FloatCompareHelper<Float32> {};
template <class U> struct CompareHelper<Float64, U> : public FloatCompareHelper<Float64> {};


/** A template for columns that use a simple array to store.
 */
template <typename T, bool has_buf = false>
class ColumnVector final : public COWHelper<ColumnVectorHelper, ColumnVector<T, has_buf>>
{
    static_assert(!IsDecimalNumber<T>);

private:
    using Self = ColumnVector;
    friend class COWHelper<ColumnVectorHelper, Self>;
    using TypeHasBuf = ColumnVector<T, true>;
    using TypeNoBuf = ColumnVector<T, false>;

    struct less;
    struct less_stable;
    struct greater;
    struct greater_stable;
    struct equals;

public:
    using ValueType = T;
    using Container = PaddedPODArray<ValueType>;

private:
    ColumnVector() {}
    ColumnVector(const size_t n) : data(n) {}
    ColumnVector(const size_t n, const ValueType x) : data(n, x) {}
    ColumnVector(const ColumnVector & src) {
        if constexpr (has_buf) {
            zero_copy_buf = (src.zero_copy_buf);
        } else {
            src.tryToFlushZeroCopyBufferImpl();
        }

        data = Container(src.data.begin(), src.data.end());
    }

    /// Sugar constructor.
    ColumnVector(std::initializer_list<T> il) : data{il} {}
    explicit ColumnVector(Container && data_): data(std::move(data_)) {}
    ColumnVector(ColumnVector && src) {
        if constexpr (has_buf) {
            zero_copy_buf = std::move(src.zero_copy_buf);
        } else {
            src.tryToFlushZeroCopyBufferImpl();
        }

        data = std::move(src.data);
    }

public:
    bool isNumeric() const override { return is_arithmetic_v<T>; }

    size_t size() const override
    {
        return data.size() + zero_copy_buf.size();
    }

    void insertFrom(const IColumn & src, size_t n) override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        const auto & src_vec = assert_cast<const Self &>(src);
        if (unlikely(src_vec.getZeroCopyBuf().size())) {
            src_vec.tryToFlushZeroCopyBufferImpl();
        }
        data.push_back(src_vec.data[n]);
    }

    void insertData(const char * pos, size_t) override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.emplace_back(unalignedLoad<T>(pos));
    }

    void insertDefault() override
    {

        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.push_back(T());
    }

    void insertManyDefaults(size_t length) override
    {

        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.resize_fill(data.size() + length, T());
    }

    void popBack(size_t n) override
    {

        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.resize_assume_reserved(data.size() - n);
    }

    void shrink(size_t to_size) override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash & hash) const override;

    size_t byteSize() const override
    {
        return size() * sizeof(T);
    }

    size_t byteSizeAt(size_t) const override
    {
        return sizeof(T);
    }

    size_t allocatedBytes() const override
    {
        return data.allocated_bytes();
    }

    void protect() override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.protect();
    }

    void insertValue(const T value)
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.push_back(value);
    }

    template <class U>
    constexpr int compareAtOther(size_t n, size_t m, const ColumnVector<U> & rhs, int nan_direction_hint) const
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }

        return CompareHelper<T, U>::compare(data[n], rhs.getData()[m], nan_direction_hint);
    }

    /// This method implemented in header because it could be possibly devirtualized.
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        const auto & rhs_vec = assert_cast<const Self &>(rhs_);
         if (unlikely(rhs_vec.getZeroCopyBuf().size())) {
            rhs_vec.tryToFlushZeroCopyBufferImpl();
        }
        return CompareHelper<T>::compare(data[n], rhs_vec.data[m], nan_direction_hint);
    }

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        const auto & rhs_vec = assert_cast<const Self &>(rhs);
        if (unlikely(rhs_vec.getZeroCopyBuf().size())) {
            rhs_vec.tryToFlushZeroCopyBufferImpl();
        }
        return this->template doCompareColumn<Self>(rhs_vec, rhs_row_num, row_indexes,
                                                    compare_results, direction, nan_direction_hint);
    }

    bool hasEqualValues() const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return this->template hasEqualValuesImpl<Self>();
    }

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges& equal_ranges) const override;

    void reserve(size_t n) override
    {
        data.reserve(n);
    }

    const char * getFamilyName() const override { return TypeName<T>; }
    TypeIndex getDataType() const override { return TypeId<T>; }

    MutableColumnPtr cloneResized(size_t size) const override;

    Field operator[](size_t n) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        assert(n < data.size()); /// This assert is more strict than the corresponding assert inside PODArray.
        return data[n];
    }


    void get(size_t n, Field & res) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        res = (*this)[n];
    }

    UInt64 get64(size_t n) const override;

    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;

    /// Out of range conversion is permitted.
    UInt64 NO_SANITIZE_UNDEFINED getUInt(size_t n) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        if constexpr (is_arithmetic_v<T>)
            return UInt64(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as UInt", TypeName<T>);
    }

    /// Out of range conversion is permitted.
    Int64 NO_SANITIZE_UNDEFINED getInt(size_t n) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        if constexpr (is_arithmetic_v<T>)
            return Int64(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Int", TypeName<T>);
    }

    bool getBool(size_t n) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        if constexpr (is_arithmetic_v<T>)
            return bool(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as bool", TypeName<T>);
    }

    void insert(const Field & x) override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        data.push_back(DB::get<T>(x));
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    void insertRangeSelective(const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length) override;

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    void expand(const IColumn::Filter & mask, bool inverted) override;

    ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const IColumn::Offsets & offsets) const override;

    void getExtremes(Field & min, Field & max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return this->template scatterImpl<Self>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    bool canBeInsideNullable() const override { return true; }
    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    StringRef getRawData() const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return StringRef(reinterpret_cast<const char*>(data.data()), byteSize());
    }

    StringRef getDataAt(size_t n) const override
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
    }

    bool isDefaultAt(size_t n) const override { return data[n] == T{}; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnVector<T, true>) || typeid(rhs) == typeid(ColumnVector<T, false>);
    }

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return this->template getRatioOfDefaultRowsImpl<Self>(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return this->template getNumberOfDefaultRowsImpl<Self>();
    }

    void getIndicesOfNonDefaultRows(IColumn::Offsets & indices, size_t from, size_t limit) const override
    {
        return this->template getIndicesOfNonDefaultRowsImpl<Self>(indices, from, limit);
    }

    ColumnPtr compress() const override;

    /// Replace elements that match the filter with zeroes. If inverted replaces not matched elements.
    void applyZeroMap(const IColumn::Filter & filt, bool inverted = false);

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container & getData()
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return data;
    }

    const Container & getData() const
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return data;
    }

    const T & getElement(size_t n) const
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return data[n];
    }

    T & getElement(size_t n)
    {
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
        return data[n];
    }

    // mutable_ptr<Self> transInfoNonBufColVector() {

    // }

    inline void tryToFlushZeroCopyBufferImpl() const
    {
        if (likely(zero_copy_buf.size() == 0))
            return;
        doRealFlushZCB();
    }

    void doRealFlushZCB() const
    {
        size_t initial_size = data.size();
        int delta_sz = 0;
        for(const auto &data_ref: zero_copy_buf.refs())
            delta_sz += data_ref.getSize();

        data.resize(initial_size + delta_sz);
        char* data_addr = reinterpret_cast<char*>(&data[initial_size]);
        for(const DataRef<T> &data_ref: zero_copy_buf.refs())
        {
            ::memcpy(data_addr, reinterpret_cast<const char*>(data_ref.getData()), data_ref.getSize()*sizeof(T));
            data_addr += data_ref.getSize()*sizeof(T);
        }
        zero_copy_buf.clear();
    }

    void tryToFlushZeroCopyBuffer() const override
    {
        tryToFlushZeroCopyBufferImpl();
    }

    void copyDataFromStart(char *dst, size_t length) const
    {
         // this func supports zero copy buffer
        size_t bytes_copied = 0;
        size_t cur_elem_cnt = 0;
        if (!data.empty())
        {
            cur_elem_cnt = std::min(length, data.size());
            ::memcpy(dst, &data[0], cur_elem_cnt * sizeof(T));
            bytes_copied += cur_elem_cnt * sizeof(T);
            length -= cur_elem_cnt;
        }
        for(const auto &data_ref: zero_copy_buf.refs())
        {
            if (length <= 0)
                break;
            cur_elem_cnt = std::min(length, data_ref.getSize());
            ::memcpy(dst + bytes_copied, data_ref.getData(), cur_elem_cnt * sizeof(T));
            bytes_copied += cur_elem_cnt * sizeof(T);
            length -= cur_elem_cnt;
        }
    }


    template <typename Type, bool o_has_buf>
    ColumnPtr useSelfAsIndex(const ColumnVector<Type, o_has_buf> & src_data_col, size_t limit) const;


    size_t getNonBufDataSize() const { return data.size();}

    const ZeroCopyBuffer<T> & getZeroCopyBuf() const { return zero_copy_buf; }

    ZeroCopyBuffer<T> & getZeroCopyBuf()  { return zero_copy_buf; }

protected:
    mutable Container data;
    mutable ZeroCopyBuffer<T> zero_copy_buf;
public:
    const bool has_zero_buf = has_buf;
};

template <typename T, bool has_buf>
template <typename Type>
ColumnPtr ColumnVector<T, has_buf>::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl();
        }
    size_t size = indexes.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = ColumnVector<T>::create(limit);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[indexes[i]];

    return res;
}

template <class TCol>
concept is_col_vector = std::is_same_v<TCol, ColumnVector<typename TCol::ValueType>>;

/// Prevent implicit template instantiation of ColumnVector for common types

extern template class ColumnVector<UInt8>;
extern template class ColumnVector<UInt16>;
extern template class ColumnVector<UInt32>;
extern template class ColumnVector<UInt64>;
extern template class ColumnVector<UInt128>;
extern template class ColumnVector<UInt256>;
extern template class ColumnVector<Int8>;
extern template class ColumnVector<Int16>;
extern template class ColumnVector<Int32>;
extern template class ColumnVector<Int64>;
extern template class ColumnVector<Int128>;
extern template class ColumnVector<Int256>;
extern template class ColumnVector<Float32>;
extern template class ColumnVector<Float64>;
extern template class ColumnVector<UUID>;
extern template class ColumnVector<IPv4>;
extern template class ColumnVector<IPv6>;

extern template class ColumnVector<UInt8, true>;
extern template class ColumnVector<UInt16, true>;
extern template class ColumnVector<UInt32, true>;
extern template class ColumnVector<UInt64, true>;
extern template class ColumnVector<UInt128, true>;
extern template class ColumnVector<UInt256, true>;
extern template class ColumnVector<Int8, true>;
extern template class ColumnVector<Int16, true>;
extern template class ColumnVector<Int32, true>;
extern template class ColumnVector<Int64, true>;
extern template class ColumnVector<Int128, true>;
extern template class ColumnVector<Int256, true>;
extern template class ColumnVector<Float32, true>;
extern template class ColumnVector<Float64, true>;
extern template class ColumnVector<UUID, true>;

}
