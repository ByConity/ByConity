/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Core/Defines.h>
#include <Common/Arena.h>
#include <Common/memcmpSmall.h>
#include <Columns/Collator.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnBitMap64.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/MaskOperations.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Common/HashTable/Hash.h>
#include <Common/WeakHash.h>
#include <common/sort.h>
#include <common/unaligned.h>
#include <common/scope_guard.h>
#include <common/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnBitMap64::ColumnBitMap64(const ColumnBitMap64 & src)
    : COWHelper<IColumn, ColumnBitMap64>(src)
    , chars(src.chars.begin(), src.chars.end())
    , offsets(src.offsets.begin(), src.offsets.end())
{
    if (!offsets.empty())
    {
        Offset last_offset = offsets.back();

        /// This will also prevent possible overflow in offset.
        if (chars.size() != last_offset)
            throw Exception("String offsets has data inconsistent with chars array", ErrorCodes::LOGICAL_ERROR);
    }
}

MutableColumnPtr ColumnBitMap64::cloneResized(size_t to_size) const
{
    auto res = ColumnBitMap64::create();

    if (to_size == 0)
        return res;

    size_t from_size = size();

    if (to_size <= from_size)
    {
        /// Just cut column.

        res->offsets.assign(offsets.begin(), offsets.begin() + to_size);
        res->chars.assign(chars.begin(), chars.begin() + offsets[to_size - 1]);
    }
    else
    {
        /// Copy column and append empty bitmaps for extra elements.

        if (from_size > 0)
        {
            res->offsets.assign(offsets.begin(), offsets.end());
            res->chars.assign(chars.begin(), chars.end());
        }

        for (size_t i = from_size; i < to_size; ++i)
        {
            res->insert(BitMap64());
        }
    }

    return res;
}

void ColumnBitMap64::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = offsets.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const UInt8 * pos = chars.data();
    UInt32 * hash_data = hash.getData().data();
    Offset prev_offset = 0;

    for (const auto & offset : offsets)
    {
        auto str_size = offset - prev_offset;
        /// Skip last zero byte.
        *hash_data = ::updateWeakHash32(pos, str_size - 1, *hash_data);

        pos += str_size;
        prev_offset = offset;
        ++hash_data;
    }
}

void ColumnBitMap64::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const ColumnBitMap64 & src_concrete = assert_cast<const ColumnBitMap64 &>(src);

    if (start + length > src_concrete.offsets.size())
        throw Exception("Parameter out of bound in IColumnString::insertRangeFrom method.",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty())
    {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    }
    else
    {
        size_t old_size = offsets.size();
        size_t prev_max_offset = offsets.back();    /// -1th index is Ok, see PaddedPODArray
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            offsets[old_size + i] = src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
    }
}

void ColumnBitMap64::insertRangeSelective(const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length)
{
    if (length == 0)
        return;

    const ColumnBitMap64 & src_concrete = static_cast<const ColumnBitMap64 &>(src);
    const Offsets & src_offsets = src_concrete.getOffsets();
    auto * src_data_start = src_concrete.chars.data();

    Offsets & cur_offsets = getOffsets();

    if (length == 0)
        return;

    size_t old_offset_size = cur_offsets.size();
    cur_offsets.resize(old_offset_size + length);

    size_t old_chars_size = chars.size();
    size_t new_chars_size = old_chars_size;
    for (size_t i = 0; i < length; ++i)
    {
        new_chars_size += src_concrete.sizeAt(selector[selector_start + i]);
    }
    chars.resize(new_chars_size);

    size_t cur_offset = cur_offsets[old_offset_size - 1];

    auto * cur_chars_start = chars.data(); // realloc memory is not allowed in the following
    for (size_t i = 0; i < length; ++i)
    {
        size_t src_pos = selector[selector_start + i];
        size_t offset = src_offsets[src_pos - 1];
        const size_t size_to_append = src_offsets[src_pos] - offset; /// -1th index is Ok, see PaddedPODArray.

        memcpySmallAllowReadWriteOverflow15(cur_chars_start + cur_offset, src_data_start + offset, size_to_append);
        cur_offset += size_to_append;
        cur_offsets[old_offset_size + i] = cur_offset;
    }
}

ColumnPtr ColumnBitMap64::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (offsets.size() == 0)
        return ColumnString::create();

    auto res = ColumnBitMap64::create();

    Chars & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    filterArraysImpl<UInt8>(chars, offsets, res_chars, res_offsets, filt, result_size_hint);
    return res;
}

void ColumnBitMap64::expand(const Filter & mask, bool inverted)
{
    auto & chars_data = getChars();
    auto & offsets_data = getOffsets();
    expandStringDataByMask(chars_data, offsets_data, mask, inverted);
}

ColumnPtr ColumnBitMap64::permute(const Permutation & perm, size_t limit) const
{
    size_t size = offsets.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnBitMap64::create();

    auto res = ColumnBitMap64::create();

    Chars & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    if (limit == size)
        res_chars.resize(chars.size());
    else
    {
        size_t new_chars_size = 0;
        for (size_t i = 0; i < limit; ++i)
            new_chars_size += sizeAt(perm[i]);
        res_chars.resize(new_chars_size);
    }

    res_offsets.resize(limit);

    Offset current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        size_t j = perm[i];
        size_t string_offset = offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}

StringRef ColumnBitMap64::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t string_size = sizeAt(n);
    size_t offset = offsetAt(n);

    StringRef res;
    res.size = sizeof(string_size) + string_size;
    char * pos = arena.allocContinue(res.size, begin);
    memcpy(pos, &string_size, sizeof(string_size));
    memcpy(pos + sizeof(string_size), &chars[offset], string_size);
    res.data = pos;

    return res;
}

const char * ColumnBitMap64::deserializeAndInsertFromArena(const char * pos)
{
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + string_size;
    chars.resize(new_size);
    memcpy(chars.data() + old_size, pos, string_size);

    offsets.push_back(new_size);
    return pos + string_size;
}

const char * ColumnBitMap64::skipSerializedInArena(const char * pos) const
{
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);
    return pos + string_size;
}

ColumnPtr ColumnBitMap64::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnBitMap64::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    if (limit == 0)
        return ColumnBitMap64::create();

    auto res = ColumnBitMap64::create();

    Chars & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    size_t new_chars_size = 0;
    for (size_t i = 0; i < limit; ++i)
        new_chars_size += sizeAt(indexes[i]);
    res_chars.resize(new_chars_size);

    res_offsets.resize(limit);

    Offset current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        size_t j = indexes[i];
        size_t string_offset = offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}

void ColumnBitMap64::compareColumn(
    const IColumn & rhs, size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
    int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnBitMap64>(assert_cast<const ColumnBitMap64 &>(rhs), rhs_row_num, row_indexes,
                                         compare_results, direction, nan_direction_hint);
}

bool ColumnBitMap64::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnString>();
}

struct ColumnBitMap64::ComparatorBase
{
    const ColumnBitMap64 & parent;
    const Collator * collator;

    ComparatorBase(const ColumnBitMap64 & parent_, const Collator * collator_ = nullptr)
        : parent(parent_), collator(collator_) {}
    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res;
        if (collator)
        {
            res = collator->compare(
                reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
                reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));
        }
        else
        {
            res = memcmpSmallAllowOverflow15(
                parent.chars.data() + parent.offsetAt(lhs), parent.sizeAt(lhs) - 1,
                parent.chars.data() + parent.offsetAt(rhs), parent.sizeAt(rhs) - 1);
        }

        return res;
    }
};

void ColumnBitMap64::getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this), DefaultSort(), DefaultPartialSort());
}

void ColumnBitMap64::updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnBitMap64::getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                            size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, &collator), DefaultSort(), DefaultPartialSort());
}

void ColumnBitMap64::updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                            size_t limit, int /*nan_direction_hint*/, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, &collator);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
    else
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, &collator), comparator_equal, DefaultSort(), DefaultPartialSort());
}

ColumnPtr ColumnBitMap64::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnBitMap64::create();

    if (0 == col_size)
        return res;

    Chars & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;
    res_chars.reserve(chars.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_string_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t string_size = offsets[i] - prev_string_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += string_size;
            res_offsets.push_back(current_new_offset);

            res_chars.resize(res_chars.size() + string_size);
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[res_chars.size() - string_size], &chars[prev_string_offset], string_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_string_offset = offsets[i];
    }

    return res;
}

void ColumnBitMap64::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnBitMap64::reserve(size_t n)
{
    offsets.reserve(n);
}

void ColumnBitMap64::getExtremes(Field & min, Field & max) const
{
    min = BitMap64();
    max = BitMap64();
}

int ColumnBitMap64::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int, const Collator & collator) const
{
    const ColumnBitMap64 & rhs = assert_cast<const ColumnBitMap64 &>(rhs_);

    return collator.compare(
        reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n),
        reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]), rhs.sizeAt(m));
}

void ColumnBitMap64::protect()
{
    getChars().protect();
    getOffsets().protect();
}

void ColumnBitMap64::validate() const
{
    if (!offsets.empty() && offsets.back() != chars.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBitMap64 validation failed: size mismatch (internal logical error) {} != {}", offsets.back(), chars.size());
}

}
