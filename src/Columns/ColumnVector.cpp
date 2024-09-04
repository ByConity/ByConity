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

#include "ColumnVector.h"

#include <pdqsort.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/MaskOperations.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/NaNUtils.h>
#include <Common/RadixSort.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <common/sort.h>
#include <common/unaligned.h>
#include <common/bit_cast.h>
#include <common/scope_guard.h>

#include <cmath>
#include <cstring>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif

namespace DB
{



namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

template <typename T, bool has_buf>
StringRef ColumnVector<T, has_buf>::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    auto * pos = arena.allocContinue(sizeof(T), begin);
    unalignedStore<T>(pos, data[n]);
    return StringRef(pos, sizeof(T));
}

template <typename T, bool has_buf>
const char * ColumnVector<T, has_buf>::deserializeAndInsertFromArena(const char * pos)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    data.emplace_back(unalignedLoad<T>(pos));
    return pos + sizeof(T);
}

template <typename T, bool has_buf>
const char * ColumnVector<T, has_buf>::skipSerializedInArena(const char * pos) const
{
    return pos + sizeof(T);
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::updateHashWithValue(size_t n, SipHash & hash) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    hash.update(data[n]);
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::updateWeakHash32(WeakHash32 & hash) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    auto s = data.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const T * begin = data.data();
    const T * end = begin + s;
    UInt32 * hash_data = hash.getData().data();

    while (begin < end)
    {
        *hash_data = intHashCRC32(*begin, *hash_data);
        ++begin;
        ++hash_data;
    }
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::updateHashFast(SipHash & hash) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    hash.update(reinterpret_cast<const char *>(data.data()), size() * sizeof(T));
}

template <typename T, bool has_buf>
struct ColumnVector<T, has_buf>::less
{
    const Self & parent;
    int nan_direction_hint;
    less(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {
        parent_.tryToFlushZeroCopyBufferImpl();
    }
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T, bool has_buf>
struct ColumnVector<T, has_buf>::less_stable
{
    const Self & parent;
    int nan_direction_hint;
    less_stable(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {
        parent_.tryToFlushZeroCopyBufferImpl();
    }
    bool operator()(size_t lhs, size_t rhs) const
    {
        if (unlikely(parent.data[lhs] == parent.data[rhs]))
            return lhs < rhs;

        if constexpr (std::is_floating_point_v<T>)
        {
            if (unlikely(std::isnan(parent.data[lhs]) && std::isnan(parent.data[rhs])))
            {
                return lhs < rhs;
            }
        }

        return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

template <typename T, bool has_buf>
struct ColumnVector<T, has_buf>::greater
{
    const Self & parent;
    int nan_direction_hint;
    greater(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {
        parent_.tryToFlushZeroCopyBufferImpl();
    }
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

template <typename T, bool has_buf>
struct ColumnVector<T, has_buf>::greater_stable
{
    const Self & parent;
    int nan_direction_hint;
    greater_stable(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {
        parent_.tryToFlushZeroCopyBufferImpl();
    }
    bool operator()(size_t lhs, size_t rhs) const
    {
        if (unlikely(parent.data[lhs] == parent.data[rhs]))
            return lhs < rhs;

        if constexpr (std::is_floating_point_v<T>)
        {
            if (unlikely(std::isnan(parent.data[lhs]) && std::isnan(parent.data[rhs])))
            {
                return lhs < rhs;
            }
        }

        return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

template <typename T, bool has_buf>
struct ColumnVector<T, has_buf>::equals
{
    const Self & parent;
    int nan_direction_hint;
    equals(const Self & parent_, int nan_direction_hint_) : parent(parent_), nan_direction_hint(nan_direction_hint_) {
        parent_.tryToFlushZeroCopyBufferImpl();
    }
    bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::equals(parent.data[lhs], parent.data[rhs], nan_direction_hint); }
};

namespace
{
    template <typename T>
    struct ValueWithIndex
    {
        T value;
        UInt32 index;
    };

    template <typename T>
    struct RadixSortTraits : RadixSortNumTraits<T>
    {
        using Element = ValueWithIndex<T>;
        using Result = size_t;

        static T & extractKey(Element & elem) { return elem.value; }
        static size_t extractResult(Element & elem) { return elem.index; }
    };
}


template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t s = data.size();
    res.resize(s);

    if (s == 0)
        return;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        for (size_t i = 0; i < s; ++i)
            res[i] = i;

        if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
            partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
            partial_sort(res.begin(), res.begin() + limit, res.end(), less_stable(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
            partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
            partial_sort(res.begin(), res.begin() + limit, res.end(), greater_stable(*this, nan_direction_hint));
    }
    else
    {
        /// A case for radix sort
        /// LSD RadixSort is stable
        if constexpr (is_arithmetic_v<T> && !is_big_int_v<T>)
        {
            bool reverse = direction == IColumn::PermutationSortDirection::Descending;
            bool ascending = direction == IColumn::PermutationSortDirection::Ascending;
            bool sort_is_stable = stability == IColumn::PermutationSortStability::Stable;

            /// TODO: LSD RadixSort is currently not stable if direction is descending, or value is floating point
            bool use_radix_sort = (sort_is_stable && ascending && !std::is_floating_point_v<T>) || !sort_is_stable;

            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (s >= 256 && s <= std::numeric_limits<UInt32>::max() && use_radix_sort)
            {
                PaddedPODArray<ValueWithIndex<T>> pairs(s);
                for (UInt32 i = 0; i < UInt32(s); ++i)
                    pairs[i] = {data[i], i};

                RadixSort<RadixSortTraits<T>>::executeLSD(pairs.data(), s, reverse, res.data());

                /// Radix sort treats all NaNs to be greater than all numbers.
                /// If the user needs the opposite, we must move them accordingly.
                if (std::is_floating_point_v<T> && nan_direction_hint < 0)
                {
                    size_t nans_to_move = 0;

                    for (size_t i = 0; i < s; ++i)
                    {
                        if (isNaN(data[res[reverse ? i : s - 1 - i]]))
                            ++nans_to_move;
                        else
                            break;
                    }

                    if (nans_to_move)
                    {
                        std::rotate(std::begin(res), std::begin(res) + (reverse ? nans_to_move : s - nans_to_move), std::end(res));
                    }
                }
                return;
            }
        }

        /// Default sorting algorithm.
        for (size_t i = 0; i < s; ++i)
            res[i] = i;

        if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
            std::sort(res.begin(), res.end(), less(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
            std::sort(res.begin(), res.end(), less_stable(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
            std::sort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
            std::sort(res.begin(), res.end(), greater_stable(*this, nan_direction_hint));
    }
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            less(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            DefaultSort(), DefaultPartialSort());
    }
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            less_stable(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            DefaultSort(), DefaultPartialSort());
    }
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            greater(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            DefaultSort(), DefaultPartialSort());
    }
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
    {
        this->updatePermutationImpl(
            limit, res, equal_ranges,
            greater_stable(*this, nan_direction_hint),
            equals(*this, nan_direction_hint),
            DefaultSort(), DefaultPartialSort());
    }
}

template <typename T, bool has_buf>
MutableColumnPtr ColumnVector<T, has_buf>::cloneResized(size_t size) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    auto res =ColumnVector<T>::create();

    if (size > 0)
    {
        auto & new_col = static_cast<ColumnVector<T> &>(*res);
        auto & col_data = new_col.getData();
        col_data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(col_data.data(), data.data(), count * sizeof(T));

        if (size > count)
            memset(static_cast<void *>(&col_data[count]), 0, (size - count) * sizeof(ValueType));
    }

    return res;
}

template <typename T, bool has_buf>
UInt64 ColumnVector<T, has_buf>::get64(size_t n [[maybe_unused]]) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if constexpr (is_arithmetic_v<T>)
        return bit_cast<UInt64>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as UInt64", TypeName<T>);
}

template <typename T, bool has_buf>
inline Float64 ColumnVector<T, has_buf>::getFloat64(size_t n [[maybe_unused]]) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if constexpr (is_arithmetic_v<T>)
        return static_cast<Float64>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Float64", TypeName<T>);
}

template <typename T, bool has_buf>
Float32 ColumnVector<T, has_buf>::getFloat32(size_t n [[maybe_unused]]) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if constexpr (is_arithmetic_v<T>)
        return static_cast<Float32>(data[n]);
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Float32", TypeName<T>);
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    // this func supports zero copy buffer 
    const ColumnVector & src_vec = assert_cast<const ColumnVector &>(src);

    if (unlikely(start + length > src_vec.size()))
        throw Exception("Parameters start = "
            + toString(start) + ", length = "
            + toString(length) + " are out of bound in ColumnVector<T, has_buf>::insertRangeFrom method"
            " (data.size() = " + toString(src_vec.size()) + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = size();

    if (start || src_vec.getZeroCopyBuf().size() == 0 
        // || !has_zero_buf
        ) 
    { // only zerocpy buf class can use zerocpy buf
        if constexpr (has_buf) {
            tryToFlushZeroCopyBufferImpl() ;
        }
        src_vec.tryToFlushZeroCopyBufferImpl();
        data.resize(old_size + length);
        ::memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(T));
    } else { // branch condition: start == 0 && src_vec.getZeroCopyBuf().size() != 0

        if (
            // has_zero_buf && 
            src_vec.getNonBufDataSize() == 0 && length == src_vec.size()) {
            for(const auto &ref: src_vec.getZeroCopyBuf().refs()) {
                zero_copy_buf.add(ref.getData(), ref.getSize(), ref.getCellHolder());
            }

        } else {
            if constexpr (has_buf) {
                tryToFlushZeroCopyBufferImpl() ;
            }
            data.resize(old_size + length);
            src_vec.copyDataFromStart(reinterpret_cast<char *>(data.data() + old_size), length);
        }
        
    }

}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::insertRangeSelective(const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t old_size = data.size();
    data.resize(old_size + length);
    const auto & src_vec = (static_cast<const Self &>(src));
    if (unlikely(src_vec.getZeroCopyBuf().size())) {
        src_vec.tryToFlushZeroCopyBufferImpl();
    }
    const auto & src_data = src_vec.getData();
    for (size_t i = 0; i < length; ++i)
    {
        data[old_size + i] = src_data[selector[selector_start + i]];
    }
}

template <typename T>
inline void filterImpl(const UInt8 * &filt_pos, size_t cur_size, const T * data_pos, PaddedPODArray<T> & res_data) 
{
  
    // const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + cur_size;
    // const T * data_pos = data.data();

    /** A slightly more optimized version.
    * Based on the assumption that often pieces of consecutive values
    *  completely pass or do not pass the filter.
    * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
    */
    static constexpr size_t SIMD_BYTES = 64;
    const UInt8 * filt_end_aligned = filt_pos + cur_size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_aligned)
    {
        UInt64 mask = bytes64MaskToBits64Mask(filt_pos);

        if (0xffffffffffffffff == mask)
        {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
            while (mask)
            {
                size_t index = __builtin_ctzll(mask);
                res_data.push_back(data_pos[index]);
#ifdef __BMI__
                mask = _blsr_u64(mask);
#else
                mask = mask & (mask-1);
#endif
            }
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }
}

template <typename T, bool has_buf>
ColumnPtr ColumnVector<T, has_buf>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
     // this func supports zero copy buffer 
    size_t size = this->size();
    if (unlikely(size != filt.size()))
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size);

    auto res = ColumnVector<T>::create();
    Container & res_data = res->getData();
     if (result_size_hint)
            res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8 * filt_pos = filt.data();
    filterImpl(filt_pos, data.size(), data.data(), res_data);
    if (zero_copy_buf.size() == 0)
        return res;
    for(const auto &ref: zero_copy_buf.refs()) 
        filterImpl(filt_pos, ref.getSize(), ref.getData(), res_data);
    return res;
}


template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::expand(const IColumn::Filter & mask, bool inverted)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }

    expandDataByMask<T>(data, mask, inverted);
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::applyZeroMap(const IColumn::Filter & filt, bool inverted)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    T * data_pos = data.data();

    if (inverted)
    {
        for (; filt_pos < filt_end; ++filt_pos, ++data_pos)
            if (!*filt_pos)
                *data_pos = 0;
    }
    else
    {
        for (; filt_pos < filt_end; ++filt_pos, ++data_pos)
            if (*filt_pos)
                *data_pos = 0;
    }
}

template <typename T, bool has_buf>
ColumnPtr ColumnVector<T, has_buf>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnVector<T>::create(limit);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

template <typename T, bool has_buf>
ColumnPtr ColumnVector<T, has_buf>::index(const IColumn & indexes, size_t limit) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if (const auto * col_uint8 = typeid_cast<const ColumnVector<UInt8> *>(&indexes, false)) {
        if (col_uint8->getZeroCopyBuf().size() == 0) {
            return (*this).template indexImpl<UInt8>(col_uint8->getData(), limit);
        } else {
            return col_uint8->useSelfAsIndex(*this, limit);
        }
    } else if (const auto * col_uint16 = typeid_cast<const ColumnVector<UInt16> *>(&indexes, false)) {
        if (col_uint16->getZeroCopyBuf().size() == 0) {
            return (*this).template indexImpl<UInt16>(col_uint16->getData(), limit);
        } else {
            return col_uint16->useSelfAsIndex(*this, limit);
        }
    } else if (const auto * col_uint32 = typeid_cast<const ColumnVector<UInt32> *>(&indexes, false)) {
        if (col_uint32->getZeroCopyBuf().size() == 0) {
            return (*this).template indexImpl<UInt32>(col_uint32->getData(), limit);
        } else {
            return col_uint32->useSelfAsIndex(*this, limit);
        }
    } else if (const auto * col_uint64 = typeid_cast<const ColumnVector<UInt64> *>(&indexes, false)) {
        if (col_uint64->getZeroCopyBuf().size() == 0) {
            return (*this).template indexImpl<UInt64>(col_uint64->getData(), limit);
        } else {
             return col_uint64->useSelfAsIndex(*this, limit);
        }
    } else {
        throw Exception("Indexes column for IColumn::select must be ColumnUInt, got " + indexes.getName(),
                        ErrorCodes::LOGICAL_ERROR);
    }

    return selectIndexImpl(*this, indexes, limit);
}

template <typename T, bool has_buf>
template <typename Type, bool o_has_buf>
ColumnPtr  ColumnVector<T, has_buf>::useSelfAsIndex(const ColumnVector<Type, o_has_buf> & src_data_col, size_t limit) const
{
    // this func supports zero copy buffer
    size_t size = this->size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = ColumnVector<Type>::create(limit);
    const auto &s_data = src_data_col.getData();

    auto & res_data = res->getData();
    size_t cur_limit = std::min(limit, data.size());
    size_t i = 0;
    auto &cur_index = data;

    for (i = 0; i < cur_limit; ++i)
        res_data[i] = s_data[cur_index[i]];
    limit -= cur_limit;
    for(const auto &ref: zero_copy_buf.refs())
    {
        if (limit <= 0)
            break;
        const T* ref_cur_index = ref.getData();
        cur_limit = std::min(limit, ref.getSize());
        for(size_t j = 0; j < cur_limit; j++)
            res_data[i++] = s_data[ref_cur_index[j]];
        limit -= cur_limit;
    }
    return res;
}

template <typename T, bool has_buf>
ColumnPtr ColumnVector<T, has_buf>::replicate(const IColumn::Offsets & offsets) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    const size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == size)
        return ColumnVector<T>::create();

    auto res = ColumnVector<T>::create(offsets.back());

    auto it = res->getData().begin(); // NOLINT
    for (size_t i = 0; i < size; ++i)
    {
        const auto span_end = res->getData().begin() + offsets[i]; // NOLINT
        for (; it != span_end; ++it)
            *it = data[i];
    }

    return res;
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::gather(ColumnGathererStream & gatherer)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    gatherer.gather(*this);
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::getExtremes(Field & min, Field & max) const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t size = data.size();

    if (size == 0)
    {
        min = T(0);
        max = T(0);
        return;
    }

    bool has_value = false;

    /** Skip all NaNs in extremes calculation.
        * If all values are NaNs, then return NaN.
        * NOTE: There exist many different NaNs.
        * Different NaN could be returned: not bit-exact value as one of NaNs from column.
        */

    T cur_min = NaNOrZero<T>();
    T cur_max = NaNOrZero<T>();

    for (const T & x : data)
    {
        if (isNaN(x))
            continue;

        if (!has_value)
        {
            cur_min = x;
            cur_max = x;
            has_value = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min);
    max = NearestFieldType<T>(cur_max);
}


#pragma GCC diagnostic ignored "-Wold-style-cast"

template <typename T, bool has_buf>
ColumnPtr ColumnVector<T, has_buf>::compress() const
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    size_t source_size = data.size() * sizeof(T);

    /// Don't compress small blocks.
    if (source_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto compressed = ColumnCompressed::compressBuffer(data.data(), source_size, false);

    if (!compressed)
        return ColumnCompressed::wrap(this->getPtr());

    return ColumnCompressed::create(data.size(), compressed->size(),
        [compressed = std::move(compressed), column_size = data.size()]
        {
            auto res = ColumnVector<T>::create(column_size);
            ColumnCompressed::decompressBuffer(
                compressed->data(), res->getData().data(), compressed->size(), column_size * sizeof(T));
            return res;
        });
}

template <typename T, bool has_buf>
void ColumnVector<T, has_buf>::shrink(size_t to_size)
{
    if constexpr (has_buf) {
        tryToFlushZeroCopyBufferImpl() ;
    }
    if (to_size >= size())
        return;

    auto it_begin = data.begin() + to_size;
    data.erase(it_begin, data.end());
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<UInt256>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Int128>;
template class ColumnVector<Int256>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
template class ColumnVector<UUID>;
template class ColumnVector<IPv4>;
template class ColumnVector<IPv6>;

template class ColumnVector<UInt8, true>;
template class ColumnVector<UInt16, true>;
template class ColumnVector<UInt32, true>;
template class ColumnVector<UInt64, true>;
template class ColumnVector<UInt128, true>;
template class ColumnVector<UInt256, true>;
template class ColumnVector<Int8, true>;
template class ColumnVector<Int16, true>;
template class ColumnVector<Int32, true>;
template class ColumnVector<Int64, true>;
template class ColumnVector<Int128, true>;
template class ColumnVector<Int256, true>;
template class ColumnVector<Float32, true>;
template class ColumnVector<Float64, true>;
template class ColumnVector<UUID, true>;

}
