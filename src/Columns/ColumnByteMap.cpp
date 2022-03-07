#include <Columns/ColumnByteMap.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <common/logger_useful.h>

#include <Common/Arena.h>
#include <DataStreams/ColumnGathererStream.h>

#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h> // toString func


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


std::string ColumnByteMap::getName() const
{
    return "Map("+ getKey().getName() + "," + getValue().getName() + ")" ;
}

ColumnByteMap::ColumnByteMap(MutableColumnPtr && key_column, MutableColumnPtr && value_column, MutableColumnPtr && offsets_column)
    :keyColumn(std::move(key_column)), valueColumn(std::move(value_column)), offsets(std::move(offsets_column))
{
}

ColumnByteMap::ColumnByteMap(MutableColumnPtr && key_column, MutableColumnPtr && value_column)
    :keyColumn(std::move(key_column)), valueColumn(std::move(value_column))
{
    if (!keyColumn->empty() || !valueColumn->empty())
       throw Exception("Not empty key, value passed to ColumnByteMap, but no offsets passed " + toString(keyColumn->size()) + " : " + toString(valueColumn->size()),
               ErrorCodes::BAD_ARGUMENTS);
    offsets = ColumnOffsets::create();
}

MutableColumnPtr ColumnByteMap::cloneEmpty() const
{
    return ColumnByteMap::create(getKey().cloneEmpty(), getValue().cloneEmpty());
}

Field ColumnByteMap::operator[](size_t n) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);
    ByteMap res(size);

    for (size_t i=0; i<size; ++i)
    {
        res[i].first = getKey()[offset + i];
        res[i].second = getValue()[offset + i];
    }
    return res;
}

void ColumnByteMap::get(size_t n, Field & res) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);
    res = ByteMap(size);
    ByteMap & res_map = DB::get<ByteMap&>(res);
    for (size_t i=0; i<size; ++i)
    {
        getKey().get(offset+i, res_map[i].first);
        getValue().get(offset+i, res_map[i].second);
    }
}

StringRef ColumnByteMap::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::insert(const Field & x)
{
    const ByteMap & map = DB::get<const ByteMap &>(x);
    size_t size = map.size();

    for (size_t i = 0; i < size; ++i)
    {
        getKey().insert(map[i].first);
        getValue().insert(map[i].second);
    }
    getOffsets().push_back(getOffsets().back() + size);
}

void ColumnByteMap::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnByteMap & src = static_cast<const ColumnByteMap &>(src_);
    size_t size = src.sizeAt(n);
    size_t offset = src.offsetAt(n);

    getKey().insertRangeFrom(src.getKey(), offset, size);
    getValue().insertRangeFrom(src.getValue(), offset, size);
    getOffsets().push_back(getOffsets().back() + size);
}

void ColumnByteMap::insertDefault()
{
    /// NOTE 1: We can use back() even if the array is empty (due to zero -1th element in PODArray).
    /// NOTE 2: We cannot use reference in push_back, because reference get invalidated if array is reallocated.
    auto last_offset = getOffsets().back();
    getOffsets().push_back(last_offset);
}

void ColumnByteMap::popBack(size_t n)
{
    auto & offsets_ = getOffsets();
    size_t nested_n = offsets_.back() - offsetAt(offsets_.size() - n);
    if (nested_n)
    {
        getKey().popBack(nested_n);
        getValue().popBack(nested_n);
    }
    offsets_.resize_assume_reserved(offsets_.size() - n);
}

StringRef ColumnByteMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t map_size = sizeAt(n);
    size_t offset = offsetAt(n);

    char * pos = arena.allocContinue(sizeof(map_size), begin);
    memcpy(pos, &map_size, sizeof(map_size));

    size_t value_size(0);
    for (size_t i = 0; i < map_size; ++i)
    {
        value_size += getKey().serializeValueIntoArena(offset+i, arena, begin).size;
        value_size += getValue().serializeValueIntoArena(offset+i, arena, begin).size;
    }

    return StringRef(begin, sizeof(map_size) + value_size);
}

const char * ColumnByteMap::deserializeAndInsertFromArena(const char * pos)
{
    size_t map_size = *reinterpret_cast<const size_t *>(pos);
    pos += sizeof(map_size);

    for (size_t i = 0; i<map_size; ++i)
    {
        pos = getKey().deserializeAndInsertFromArena(pos);
        pos = getValue().deserializeAndInsertFromArena(pos);
    }

    getOffsets().push_back(getOffsets().back() + map_size);
    return pos;
}

const char * ColumnByteMap::skipSerializedInArena(const char * pos) const
{
    size_t map_size = unalignedLoad<size_t>(pos);
    pos += sizeof(map_size);

    for (size_t i = 0; i < map_size; ++i)
    {
        pos = getKey().skipSerializedInArena(pos);
        pos = getValue().skipSerializedInArena(pos);
    }

    return pos;
}

void ColumnByteMap::updateHashWithValue([[maybe_unused]] size_t n, [[maybe_unused]] SipHash & hash) const
{
    throw Exception("Map doesn't support updateHashWithValue", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0) return;
    const ColumnByteMap & src_concrete = static_cast<const ColumnByteMap &>(src);

    if (start + length > src_concrete.getOffsets().size())
        throw Exception("Parameter out of bound in ColumnByteMap::insertRangeFrom method.",
            ErrorCodes::BAD_ARGUMENTS);
    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.getOffsets()[start + length -1] - nested_offset;

    getKey().insertRangeFrom(src_concrete.getKey(), nested_offset, nested_length);
    getValue().insertRangeFrom(src_concrete.getValue(), nested_offset, nested_length);

    Offsets & cur_offsets = getOffsets();
    const Offsets & src_offsets = src_concrete.getOffsets();

    if (start == 0 && cur_offsets.empty())
    {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    }
    else
    {
        size_t old_size = cur_offsets.size();
        size_t prev_max_offset = old_size ? cur_offsets.back() : 0;
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            cur_offsets[old_size + i] = src_offsets[start+i] - nested_offset + prev_max_offset;
    }
}

void ColumnByteMap::insertRangeSelective(const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length)
{
    if (length == 0) return;
    
    const ColumnByteMap & src_concrete = static_cast<const ColumnByteMap &>(src);
    const IColumn & src_key_col = src_concrete.getKey();
    const IColumn & src_value_col = src_concrete.getValue();
    IColumn & cur_key_col = getKey();
    IColumn & cur_value_col = getValue();
    Offsets & cur_offsets = getOffsets();
    
    size_t old_size = cur_offsets.size();
    cur_offsets.resize(old_size + length);

    size_t cur_offset_size = cur_offsets[old_size - 1];
    for (size_t i = 0; i < length; i++)
    {
        size_t n = selector[selector_start + i];
        size_t size = src_concrete.sizeAt(n);
        size_t offset = src_concrete.offsetAt(n);

        cur_key_col.insertRangeFrom(src_key_col, offset, size);
        cur_value_col.insertRangeFrom(src_value_col, offset, size);
        cur_offsets[old_size + i] = cur_offset_size + size;
        cur_offset_size += size;
    }
}

// static method to filter Columns which was built on Offsets(similar to ColumnArray)
void ColumnByteMap::filter(const ColumnPtr& implCol, ColumnPtr& implResCol,
                              const Offsets& offsets, const Filter& filt,
                              ssize_t result_size_hint)
{
    // Handle implicit key column
    if (typeid_cast<const ColumnUInt8 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<UInt8>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnUInt16 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<UInt16>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnUInt32 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<UInt32>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnUInt64 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<UInt64>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnInt8 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Int8>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnInt16 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Int16>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnInt32 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Int32>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnInt64 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Int64>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnFloat32 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Float32>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnFloat64 *>(implCol.get()))
    {
        ColumnByteMap::filterNumber<Float64>(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnString *>(implCol.get()))
    {
        ColumnByteMap::filterString(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnNullable *>(implCol.get()))
    {
        ColumnByteMap::filterNullable(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnArray *>(implCol.get()))
    {
        ColumnByteMap::filterArray(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else if (typeid_cast<const ColumnLowCardinality *>(implCol.get()))
    {
        ColumnByteMap::filterLowCardinality(implCol, implResCol, offsets, filt, result_size_hint);
    }
    else
    {
        throw Exception("ColumnByteMap doesn't support this implicit type", ErrorCodes::NOT_IMPLEMENTED);
    }
}

ColumnPtr ColumnByteMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    size_t size = getOffsets().size();
    if (size == 0)
    {
        return ColumnByteMap::create(keyColumn, valueColumn);
    }
    // There are two implicit columns that need to be filtered, and Offsets need adjustments.
    auto res = ColumnByteMap::create(keyColumn->cloneEmpty(), valueColumn->cloneEmpty());

    Offsets & res_offsets = res->getOffsets();

    // Handle implicit key column
    ColumnByteMap::filter(keyColumn, res->getKeyPtr(), getOffsets(), filt, result_size_hint);

    // Handle implicit value column
    ColumnByteMap::filter(valueColumn, res->getValuePtr(), getOffsets(), filt, result_size_hint);

    // Handle Offsets explicitly
    size_t current_offset = 0;
    for (size_t i = 0; i<size; ++i)
    {
        if (filt[i])
        {
            current_offset += sizeAt(i);
            res_offsets.push_back(current_offset);
        }
    }

    return res;
}

// template processing based on key value types
template<typename T>
void ColumnByteMap::filterNumber(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                    const Offsets& offsets, const Filter & filt,
                                    ssize_t result_size_hint)
{
    filterArraysImplOnlyData<T>(static_cast<const ColumnVector<T> &>(*implCol).getData(),
                                offsets,
                                static_cast<ColumnVector<T>&>(implResCol->assumeMutableRef()).getData(),
                                filt,
                                result_size_hint);
}

void ColumnByteMap::filterArray(const ColumnPtr & implCol, ColumnPtr & implResCol, const Offsets & offsets,
                                const Filter & filt, ssize_t result_size_hint)
{
    // Construct a new filter for filtering column
    Filter new_filter;
    Offset prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        size_t array_size = offsets[i] - prev_offset;
        size_t old_size = new_filter.size();
        if (filt[i])
            new_filter.resize_fill(old_size + array_size, 1);
        else
            new_filter.resize_fill(old_size + array_size, 0);

        prev_offset += array_size;
    }

    const ColumnArray & src_array = typeid_cast<const ColumnArray &>(*implCol);
    ColumnPtr res_ptr = src_array.filter(new_filter, result_size_hint);
    implResCol = std::move(res_ptr);
}

void ColumnByteMap::filterLowCardinality(const ColumnPtr & implCol, ColumnPtr & implResCol, const Offsets & offsets,
                            const Filter & filt, ssize_t result_size_hint)
{
    // Construct a new filter for filtering column
    Filter new_filter;
    Offset prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        size_t array_size = offsets[i] - prev_offset;
        size_t old_size = new_filter.size();
        if (filt[i])
            new_filter.resize_fill(old_size + array_size, 1);
        else
            new_filter.resize_fill(old_size + array_size, 0);

        prev_offset += array_size;
    }

    const ColumnLowCardinality & src_col = typeid_cast<const ColumnLowCardinality &>(*implCol);
    ColumnPtr res_ptr = src_col.filter(new_filter, result_size_hint);
    implResCol = std::move(res_ptr);
}

void ColumnByteMap::filterString(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                    const Offsets& offsets, const Filter & filt,
                                    ssize_t result_size_hint)
{
    size_t col_size = offsets.size();
    const ColumnString & src_string = typeid_cast<const ColumnString &>(*implCol);
    const ColumnString::Chars & src_chars = src_string.getChars();
    const Offsets & src_string_offsets = src_string.getOffsets();
    const Offsets & src_offsets = offsets;

    ColumnString::Chars & res_chars = typeid_cast<ColumnString &>(implResCol->assumeMutableRef()).getChars();
    Offsets& res_string_offsets = typeid_cast<ColumnString &>(implResCol->assumeMutableRef()).getOffsets();

    if (result_size_hint < 0)
    {
        res_chars.reserve(src_chars.size());
        res_string_offsets.reserve(src_string_offsets.size());
    }

    Offset prev_src_offset = 0;
    Offset prev_src_string_offset = 0;

    Offset prev_res_offset = 0;
    Offset prev_res_string_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        /// Number of rows in the array.
        size_t array_size = src_offsets[i] - prev_src_offset;

        if (filt[i])
        {
            /// If the array is not empty - copy content.
            if (array_size)
            {
                size_t chars_to_copy = src_string_offsets[array_size + prev_src_offset - 1] - prev_src_string_offset;
                size_t res_chars_prev_size = res_chars.size();
                res_chars.resize(res_chars_prev_size + chars_to_copy);
                memcpy(&res_chars[res_chars_prev_size], &src_chars[prev_src_string_offset], chars_to_copy);

                for (size_t j = 0; j < array_size; ++j)
                    res_string_offsets.push_back(src_string_offsets[j + prev_src_offset] + prev_res_string_offset - prev_src_string_offset);

                prev_res_string_offset = res_string_offsets.back();
            }

            prev_res_offset += array_size;
        }

        if (array_size)
        {
            prev_src_offset += array_size;
            prev_src_string_offset = src_string_offsets[prev_src_offset - 1];
        }
    }
}

void ColumnByteMap::filterNullable(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                      const Offsets& offsets, const Filter & filt,
                                      ssize_t result_size_hint)
{
    const ColumnNullable & nullable_elems = static_cast<const ColumnNullable &>(*implCol);
    ColumnNullable & nullable_res = static_cast<ColumnNullable &>(implResCol->assumeMutableRef());

    auto& src_nested_column = nullable_elems.getNestedColumnPtr();
    auto& src_null_ind = nullable_elems.getNullMapColumnPtr();

    auto& res_nested_column = nullable_res.getNestedColumnPtr();
    auto& res_null_ind = nullable_res.getNullMapColumnPtr();

    // Handle nested column
    ColumnByteMap::filter(src_nested_column, res_nested_column, offsets, filt, result_size_hint);
    // Handle null indicator
    ColumnByteMap::filter(src_null_ind, res_null_ind, offsets, filt, result_size_hint);
}

/** TODO: double check the logic later **/
ColumnPtr ColumnByteMap::permute(const Permutation & perm, size_t limit) const
{
    size_t size = getOffsets().size();
    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
    {
        return ColumnByteMap::create(keyColumn, valueColumn);
    }

    Permutation nested_perm(getOffsets().back());

    auto res = ColumnByteMap::create(keyColumn->cloneEmpty(), valueColumn->cloneEmpty());
    Offsets& res_offsets = res->getOffsets();
    res_offsets.resize(limit);

    size_t current_offset = 0;
    for (size_t i = 0; i < limit; ++i)
    {
        for (size_t j = 0; j < sizeAt(perm[i]); ++j)
            nested_perm[current_offset + j] = offsetAt(perm[i]) + j;
        current_offset += sizeAt(perm[i]);
        res_offsets[i] = current_offset;
    }

    if (current_offset != 0)
    {
        res->keyColumn = keyColumn->permute(nested_perm, current_offset);
        res->valueColumn = valueColumn->permute(nested_perm, current_offset);
    }

    return res;
}

ColumnPtr ColumnByteMap::index(const IColumn & indexes, size_t limit) const
{
    auto key_index_column = keyColumn->index(indexes, limit);
    auto value_index_column = valueColumn->index(indexes, limit);
    return ColumnByteMap::create(key_index_column, value_index_column);
}

void ColumnByteMap::getPermutation([[maybe_unused]] bool reverse,
                               [[maybe_unused]] size_t limit,
                               [[maybe_unused]] int nan_direction_hint,
                               [[maybe_unused]] Permutation & res) const
{
    throw Exception("ColumnByteMap::getPermutation not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::updatePermutation(bool, size_t, int, IColumn::Permutation &, EqualRanges &) const
{
    throw Exception("ColumnByteMap::updatePermutation not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

ColumnPtr ColumnByteMap::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    auto res = ColumnByteMap::create(keyColumn->cloneEmpty(), valueColumn->cloneEmpty());
    if (col_size == 0)
    {
        return res;
    }

    Offsets & res_offsets = res->getOffsets();

    ColumnByteMap::replicate(keyColumn, res->getKeyPtr(), getOffsets(), replicate_offsets);
    ColumnByteMap::replicate(valueColumn, res->getValuePtr(), getOffsets(), replicate_offsets);

    // Handle offsets explicitly
    const Offsets & src_offsets = getOffsets();
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_data_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += value_size;
            res_offsets.push_back(current_new_offset);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }

    return res;
}

void ColumnByteMap::replicate(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                 const Offsets& offsets, const Offsets& replicate_offsets)
{
    // Handle implicit key column
    if (typeid_cast<const ColumnUInt8 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<UInt8>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt16 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<UInt16>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt32 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<UInt32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt64 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<UInt64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt8 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Int8>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt16 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Int16>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt32 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Int32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt64 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Int64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnFloat32 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Float32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnFloat64 *>(implCol.get()))
    {
        ColumnByteMap::replicateNumber<Float64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnString *>(implCol.get()))
    {
        ColumnByteMap::replicateString(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnNullable *>(implCol.get()))
    {
        ColumnByteMap::replicateNullable(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnArray *>(implCol.get()))
    {
        ColumnByteMap::replicateArray(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnLowCardinality *>(implCol.get()))
    {
        ColumnByteMap::replicateLowCardinality(implCol, implResCol, offsets, replicate_offsets);
    }
    else
    {
        throw Exception("ColumnByteMap doesn't support this implicit type", ErrorCodes::NOT_IMPLEMENTED);
    }

}

void ColumnByteMap::replicateLowCardinality(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                const Offsets& src_offsets, const Offsets& replicate_offsets)
{
    if (implCol->size() == 0)
        return;

    auto * src_lc = typeid_cast<const ColumnLowCardinality *>(implCol.get());
    auto * lc = typeid_cast<const ColumnLowCardinality *>(implResCol.get());

    auto & index = const_cast<ColumnPtr& >(lc->getIndexesPtr());
    auto const & src_index = src_lc->getIndexesPtr();
    ColumnByteMap::replicate(src_index, index, src_offsets, replicate_offsets);

    implResCol = ColumnLowCardinality::create(src_lc->getDictionaryPtr(), index);
}

template <typename T>
void ColumnByteMap::replicateNumber(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                const Offsets& src_offsets, const Offsets& replicate_offsets)
{
    size_t col_size = src_offsets.size();

    const typename ColumnVector<T>::Container& src_data =
        typeid_cast<const ColumnVector<T> &>(*implCol).getData();

    typename ColumnVector<T>::Container& res_data =
        typeid_cast<ColumnVector<T>&>(implResCol->assumeMutableRef()).getData();

    res_data.reserve(implCol->size() / col_size * replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_data_offset = 0;
    Offset current_new_offset = 0;
    for (size_t i = 0; i<col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_data_offset;
        for (size_t j = 0; j<size_to_replicate; ++j)
        {
            current_new_offset += value_size;
            res_data.resize(res_data.size() + value_size);

            memcpy(&res_data[res_data.size() - value_size], &src_data[prev_data_offset], value_size * sizeof(T));
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_data_offset = src_offsets[i];
    }
}

template <typename T>
void ColumnByteMap::replicateArrayNumber(const ColumnPtr & implCol, ColumnPtr & implResCol, const Offsets & src_offsets, const Offsets & replicate_offsets)
{
    size_t col_size = src_offsets.size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    const ColumnArray & src_array_num = typeid_cast<const ColumnArray &>(*implCol);
    const typename ColumnVector<T>::Container & src_data = typeid_cast<const ColumnVector<T> &>(src_array_num.getData()).getData();
    const Offsets & src_num_offsets = src_array_num.getOffsets();

    ColumnArray & res_array_num = typeid_cast<ColumnArray &>(implResCol->assumeMutableRef());
    typename ColumnVector<T>::Container & res_data = typeid_cast<ColumnVector<T> &>(res_array_num.getData()).getData();
    Offsets & res_num_offsets = res_array_num.getOffsets();

    res_data.reserve(src_data.size() / col_size * replicate_offsets.back());
    res_num_offsets.reserve(src_num_offsets.size() / col_size * replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_src_offset = 0;
    Offset prev_src_num_offset = 0;
    Offset current_res_offset = 0;
    Offset current_res_num_offset = 0;

    for (size_t i = 0; i< col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_src_offset;
        size_t sum_num_size = value_size == 0 ? 0 : (src_num_offsets[prev_src_offset + value_size - 1] - prev_src_num_offset);
        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_res_offset += value_size;
            size_t prev_src_num_offset_local = prev_src_num_offset;
            for (size_t k = 0; k < value_size; ++k)
            {
                size_t num_size = src_num_offsets[k + prev_src_offset] - prev_src_num_offset_local;
                current_res_num_offset += num_size;
                res_num_offsets.push_back(current_res_num_offset);
                prev_src_num_offset_local += num_size;
            }

            if (sum_num_size)
            {
                res_data.resize(res_data.size() + sum_num_size);
                memcpy(&res_data[res_data.size() - sum_num_size], &src_data[prev_src_num_offset], sum_num_size * sizeof(T));
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_num_offset += sum_num_size;
    }
}

void ColumnByteMap::replicateArrayString(const ColumnPtr & implCol, ColumnPtr & implResCol, const Offsets & src_offsets, const Offsets & replicate_offsets)
{
    size_t col_size = src_offsets.size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    const ColumnArray & src_array_string = typeid_cast<const ColumnArray &>(*implCol);
    const ColumnString & src_string = typeid_cast<const ColumnString &>(src_array_string.getData());
    const ColumnString::Chars & src_chars = src_string.getChars();
    const Offsets & src_array_offsets = src_array_string.getOffsets();
    const Offsets & src_string_offsets = src_string.getOffsets();

    ColumnArray & res_array_string = typeid_cast<ColumnArray &>(implResCol->assumeMutableRef());
    ColumnString & res_string = typeid_cast<ColumnString &>(res_array_string.getData());
    ColumnString::Chars & res_chars = res_string.getChars();
    Offsets & res_array_offsets = res_array_string.getOffsets();
    Offsets & res_string_offsets = res_string.getOffsets();

    res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
    res_array_offsets.reserve(src_array_offsets.size() / col_size * replicate_offsets.back());
    res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_src_offset = 0;
    Offset prev_src_array_offset = 0;
    Offset prev_src_string_offset = 0;
    Offset current_res_array_offset = 0;
    Offset current_res_string_offset = 0;

    for (size_t i = 0; i< col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_src_offset;
        size_t sum_string_size = value_size == 0 ? 0 : (src_array_offsets[prev_src_offset + value_size - 1] - prev_src_array_offset);
        size_t sum_chars_size = sum_string_size == 0 ? 0 : (src_string_offsets[prev_src_array_offset + sum_string_size - 1] - prev_src_string_offset);

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            size_t prev_src_array_offset_local = prev_src_array_offset;
            size_t prev_src_string_offset_local = prev_src_string_offset;
            for (size_t k = 0; k < value_size; ++k)
            {
                // Add array offset
                size_t string_size = src_array_offsets[k + prev_src_offset] - prev_src_array_offset_local;
                current_res_array_offset += string_size;
                res_array_offsets.push_back(current_res_array_offset);

                // Add string offset
                for (size_t l = 0; l < string_size; ++l)
                {
                    size_t chars_size = src_string_offsets[l + prev_src_array_offset_local] - prev_src_string_offset_local;
                    current_res_string_offset += chars_size;
                    res_string_offsets.push_back(current_res_string_offset);
                    prev_src_string_offset_local += chars_size;
                }

                prev_src_array_offset_local += string_size;
            }

            if (sum_chars_size)
            {
                res_chars.resize(res_chars.size() + sum_chars_size);
                memcpySmallAllowReadWriteOverflow15(&res_chars[res_chars.size() - sum_chars_size], &src_chars[prev_src_string_offset], sum_chars_size);
            }
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_array_offset += sum_string_size;
        prev_src_string_offset += sum_chars_size;
    }

}

void ColumnByteMap::replicateArray(const ColumnPtr & implCol, ColumnPtr & implResCol, const Offsets & offsets, const Offsets & replicate_offsets)
{
    // Handle implicit key column
    const ColumnArray & col_array = typeid_cast<const ColumnArray &>(*implCol);
    if (typeid_cast<const ColumnUInt8 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<UInt8>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt16 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<UInt16>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt32 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<UInt32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnUInt64 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<UInt64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt8 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Int8>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt16 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Int16>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt32 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Int32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnInt64 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Int64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnFloat32 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Float32>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnFloat64 *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayNumber<Float64>(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnString *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateArrayString(implCol, implResCol, offsets, replicate_offsets);
    }
    else if (typeid_cast<const ColumnNullable *>(col_array.getDataPtr().get()))
    {
        ColumnByteMap::replicateNullable(implCol, implResCol, offsets, replicate_offsets);
    }
    else
    {
        throw Exception("ColumnByteMap doesn't support this implicit type", ErrorCodes::NOT_IMPLEMENTED);
    }
}

void ColumnByteMap::replicateString(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                const Offsets& src_offsets, const Offsets& replicate_offsets)
{
    size_t col_size = src_offsets.size();
    const ColumnString & src_string = typeid_cast<const ColumnString &>(*implCol);
    const ColumnString::Chars & src_chars = src_string.getChars();
    const Offsets & src_string_offsets = src_string.getOffsets();

    ColumnString::Chars& res_chars = typeid_cast<ColumnString &>(implResCol->assumeMutableRef()).getChars();
    Offsets & res_string_offsets = typeid_cast<ColumnString &>(implResCol->assumeMutableRef()).getOffsets();

    res_chars.reserve(src_chars.size() / col_size * replicate_offsets.back());
    res_string_offsets.reserve(src_string_offsets.size() / col_size * replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_src_offset = 0;
    Offset prev_src_string_offset = 0;
    Offset current_res_offset = 0;
    Offset current_res_string_offset = 0;

    for (size_t i = 0; i<col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t value_size = src_offsets[i] - prev_src_offset;
        size_t sum_chars_size = value_size == 0 ? 0 : (src_string_offsets[prev_src_offset + value_size - 1] - prev_src_string_offset);

        for (size_t j = 0; j< size_to_replicate; ++j)
        {
            current_res_offset += value_size;
            size_t prev_src_string_offset_local = prev_src_string_offset;
            for (size_t k = 0; k < value_size; ++k)
            {
                /// Size of one row.
                size_t chars_size = src_string_offsets[k + prev_src_offset] - prev_src_string_offset_local;
                current_res_string_offset += chars_size;
                res_string_offsets.push_back(current_res_string_offset);
                prev_src_string_offset_local += chars_size;
            }
            /// Copies the characters of the array of rows.
            res_chars.resize(res_chars.size() + sum_chars_size);
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[res_chars.size() - sum_chars_size], &src_chars[prev_src_string_offset], sum_chars_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_src_offset = src_offsets[i];
        prev_src_string_offset += sum_chars_size;
    }
}

void ColumnByteMap::replicateNullable(const ColumnPtr& implCol, ColumnPtr& implResCol,
                                         const Offsets& offsets, const Offsets& replicate_offsets)
{
    const ColumnNullable & nullable_elems = static_cast<const ColumnNullable &>(*implCol);
    ColumnNullable & nullable_res = static_cast<ColumnNullable &>(implResCol->assumeMutableRef());

    auto& src_nested_column = nullable_elems.getNestedColumnPtr();
    auto& src_null_ind = nullable_elems.getNullMapColumnPtr();

    auto& res_nested_column = nullable_res.getNestedColumnPtr();
    auto& res_null_ind = nullable_res.getNullMapColumnPtr();

    ColumnByteMap::replicate(src_nested_column, res_nested_column, offsets, replicate_offsets);

    ColumnByteMap::replicate(src_null_ind, res_null_ind, offsets, replicate_offsets);
}

MutableColumns ColumnByteMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    return scatterImpl<ColumnByteMap>(num_columns, selector);
}

int ColumnByteMap::compareAt([[maybe_unused]]size_t n,
                         [[maybe_unused]]size_t m,
                         [[maybe_unused]]const IColumn & rhs,
                         [[maybe_unused]]int nan_direction_hint) const
{
    throw Exception("ColumnByteMap::compareAt not implemented yet!", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::compareColumn(const IColumn &, size_t,
                                PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &,
                                int, int) const
{
    throw Exception("ColumnByteMap::compareColumn not implemented yet!", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnByteMap::reserve(size_t n)
{
    getOffsets().reserve(n);
    getKey().reserve(n); // conservative reserve n slot
    getValue().reserve(n);
}

size_t ColumnByteMap::byteSize() const
{
    return getKey().byteSize() + getValue().byteSize() + sizeof(getOffsets()[0]) * size();
}

size_t ColumnByteMap::byteSizeAt(size_t n) const
{
    return getKey().byteSizeAt(n) + getValue().byteSizeAt(n) + sizeof(getOffsets()[0]);
}



size_t ColumnByteMap::allocatedBytes() const
{
    return getKey().allocatedBytes() + getValue().allocatedBytes() + getOffsets().allocated_bytes();
}

void ColumnByteMap::getExtremes([[maybe_unused]] Field & min, [[maybe_unused]] Field & max) const
{
    throw Exception("ColumnByteMap::getExtremes not implemented yet!", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::protect()
{
    getKey().protect();
    getValue().protect();
    getOffsets().protect();
}


void ColumnByteMap::forEachSubcolumn(ColumnCallback callback)
{
    callback(offsets);
    callback(keyColumn);
    callback(valueColumn);
}

/**
 * The low cardinality value column compose by a dictionary and a index array. And the memory layout as the following:
 *    dict                 index
 * |"abc"   |             | 1 |
 * |"abcd"  |             | 2 |
 * |"abcde" |             | 3 |
 *                        | 1 |
 *                        | 3 |
 * And usually, the index would be type of uint_8, the dict be the type of ColumnUnique<String>
 */
ColumnPtr ColumnByteMap::getValueColumnByKeyForLC(const StringRef & key, size_t rows_to_read) const
{
    // check whether valueColumn is Nullable
    ColumnPtr res = valueColumn->cloneEmpty();
    size_t col_size = size();

    const Offsets & offsets_ = getOffsets();

    if (col_size == 0)
        return res;
    auto & res_col = res->assumeMutableRef();
    res_col.reserve(col_size);
    size_t offset = 0;

    size_t row_size = offsets_[0];
    bool found_key = false;

    size_t rows = rows_to_read == 0 ? col_size: std::min(rows_to_read, col_size);
    for (size_t i = 0; i < rows; ++i)
    {
        found_key = false;
        offset = offsets_[i - 1];
        row_size = offsets_[i] - offset;

        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = keyColumn->getDataAt(offset + r);
            if (tmp_key == key)
            {
                found_key = true;
                res_col.insertFrom(*valueColumn, offset + r);
                break;
            }
        }

        if (!found_key)
        {
            res_col.insert(Null());
        }
    }

    return res;
}

/** Generic implementation of get implicit value column based on key value.
 * TODO: specialize this function for Number type and String type.
 */
ColumnPtr ColumnByteMap::getValueColumnByKey(const StringRef & key, size_t rows_to_read) const
{
    // check whether if lc column type
    if (valueColumn->lowCardinality())
        return getValueColumnByKeyForLC(key, rows_to_read);

    // check whether valueColumn is Nullable
    ColumnPtr res = valueColumn->cloneEmpty();
    size_t col_size = size();

    const Offsets & offsets_ = getOffsets();
    res = makeNullable(res);

    if (col_size == 0)
        return res;
    auto & res_col = res->assumeMutableRef();
    res_col.reserve(col_size);
    size_t offset = 0;

    size_t row_size = offsets_[0];
    bool found_key = false;

    size_t rows = rows_to_read == 0 ? col_size: std::min(rows_to_read, col_size);
    for (size_t i = 0; i < rows; ++i)
    {
        found_key = false;
        offset = offsets_[i - 1];
        row_size = offsets_[i] - offset;

        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = keyColumn->getDataAt(offset + r);
            if (tmp_key == key)
            {
                found_key = true;
                static_cast<ColumnNullable &>(res_col).getNestedColumn().insertFrom(*valueColumn, offset + r);
                static_cast<ColumnNullable &>(res_col).getNullMapData().push_back(0);
                break;
            }
        }

        if (!found_key)
        {
            res_col.insert(Null());
        }
    }

    return res;
}

/**
 * This routine will reconsturct MAP column based on its implicit columns.
 * \param implKeyValues is the collection of {key_name, {offset, key_column}}
 * and this routine will be compatible with non-zero offset. non-zero offset
 * could happen for scenario that map column and implicit column are both
 * referenced in the same query, e.g.
 * select map from table where map{key} =1
 * In the above case, MergeTreeReader will use the same stream __map__key for
 * both keys, while reconstructing(append) map column, size of implicit column
 * __map__key was accumulated.
 */
void ColumnByteMap::fillByExpandedColumns(
     const DataTypeByteMap& mapType,
     const std::map<String, std::pair<size_t, const IColumn*> >& implKeyValues
)
{
    // Append to ends of this ColumnByteMap
    if (implKeyValues.empty()) return;

    if (implKeyValues.begin()->second.second->size() < implKeyValues.begin()->second.first)
    {
        throw Exception("MAP implicit key size is slow than offset " +
                        toString(implKeyValues.begin()->second.second->size()) + " " + toString(implKeyValues.begin()->second.first),
                        ErrorCodes::LOGICAL_ERROR);
    }

    size_t rows = implKeyValues.begin()->second.second->size() - implKeyValues.begin()->second.first;


    IColumn& keyCol = getKey();
    IColumn& valueCol = getValue();
    Offsets& offsets_ = getOffsets();
    size_t allKeysNum = implKeyValues.size();
    std::vector<Field> keys;
    keys.reserve(allKeysNum);

    auto& keyType = mapType.getKeyType();

    //Intepreter Key columns
    for (auto& kv : implKeyValues)
    {
        keys.push_back(keyType->stringToVisitorField(kv.first));
        // Sanity check that all implicit columns(plus offset) are the same size
        if (rows + kv.second.first != kv.second.second->size())
        {
            throw Exception("implicit column is with different size " +
                            toString(kv.second.second->size()) + " " + toString(rows + kv.second.first),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (mapType.valueTypeIsLC())
    {
        for (size_t i = 0; i< rows; i++)
        {
            size_t numKVPairs = 0;
            size_t iterIdx = 0;
            for (auto it = implKeyValues.begin(); it != implKeyValues.end(); ++it, ++iterIdx)
            {
                size_t implOffset = it->second.first;
                const ColumnLowCardinality & implValueCol = typeid_cast<const ColumnLowCardinality &>(*it->second.second);
                // ignore those input where value is NULL.
                if (implValueCol.isNullAt(i+ implOffset))  continue;

                // TODO: could be optimized
                keyCol.insert(keys[iterIdx]);
                valueCol.insert(implValueCol[i + implOffset]);
                numKVPairs++;
            }
            offsets_.push_back((offsets_.size() == 0 ? 0 : offsets_.back()) + numKVPairs);
        }
    }
    else
    {
        for (size_t i = 0; i< rows; i++)
        {
            size_t numKVPairs = 0;
            size_t iterIdx = 0;
            for (auto it = implKeyValues.begin(); it != implKeyValues.end(); ++it, ++iterIdx)
            {
                size_t implOffset = it->second.first;
                //TODO: optimize it without typeid_cast
                const ColumnNullable& implValueCol = typeid_cast<const ColumnNullable&>(*it->second.second);

                // ignore those input where value is NULL.
                if (implValueCol.isNullAt(i+ implOffset))  continue;

                // TODO: could be optimized
                keyCol.insert(keys[iterIdx]);
                valueCol.insert(implValueCol[i + implOffset]);
                numKVPairs++;
            }
            offsets_.push_back((offsets_.size() == 0 ? 0 : offsets_.back()) + numKVPairs);
        }
    }
}

void ColumnByteMap::removeKeys(const NameSet & keys)
{
    size_t col_size = size();
    if (col_size == 0)
        return;

    ColumnPtr key_res = keyColumn->cloneEmpty();
    ColumnPtr value_res = valueColumn->cloneEmpty();
    ColumnPtr offset_res = offsets->cloneEmpty();
    
    const Offsets & offsets_ = getOffsets();

    auto & key_res_col = key_res->assumeMutableRef();
    auto & value_res_col = value_res->assumeMutableRef();
    Offsets& offset_res_col = static_cast<ColumnOffsets &>(offset_res->assumeMutableRef()).getData();
    size_t offset = 0;

    size_t row_size = offsets_[0];

    for (size_t i = 0; i < col_size; ++i)
    {
        offset = offsets_[i - 1];
        row_size = offsets_[i] - offset;

        size_t numKVPairs = 0;
        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = keyColumn->getDataAt(offset + r).toString();
            if (!keys.count(tmp_key))
            {
                key_res_col.insertFrom(*keyColumn, offset + r);
                value_res_col.insertFrom(*valueColumn, offset + r);
                numKVPairs++;
            }
        }
        offset_res_col.push_back((offset_res_col.size() == 0 ? 0 : offset_res_col.back()) + numKVPairs);
    }

    keyColumn = std::move(key_res);
    valueColumn = std::move(value_res);
    offsets = std::move(offset_res);
}

/***
 * insert implicit map column from outside, 
 * only when the current ColumnByteMap has the same size with implicit columns. 
 * implicit columns should be nullable, if it is nullable, you should make nullable before this method.
 * 
 * for lowcardinality, use its full columns and insert by insertFromFullColumn;
 */
void ColumnByteMap::insertImplicitMapColumns(const std::unordered_map<String, ColumnPtr> & implicit_columns)
{
    if (implicit_columns.empty())
        return;
    
    size_t col_size = size();

    ColumnPtr key_res = keyColumn->cloneEmpty();
    ColumnPtr value_res = valueColumn->cloneEmpty();
    ColumnPtr offset_res = offsets->cloneEmpty();
    const Offsets & offsets_ = getOffsets();
    auto & key_res_col = key_res->assumeMutableRef();
    auto & value_res_col = value_res->assumeMutableRef();
    Offsets& offset_res_col = static_cast<ColumnOffsets &>(offset_res->assumeMutableRef()).getData();

    auto * value_res_col_lowcardinality = typeid_cast<ColumnLowCardinality *>(&value_res_col);
    bool is_lowcardinality_value = valueColumn->lowCardinality() && value_res_col_lowcardinality;

    std::unordered_map<String, const ColumnNullable *> nullable_columns;
    for (auto it = implicit_columns.begin(); it != implicit_columns.end(); ++it)
    {
        auto column_without_lowcardinality = recursiveRemoveLowCardinality(it->second);
        auto * column = checkAndGetColumn<ColumnNullable>(column_without_lowcardinality.get());
        nullable_columns[it->first] = column;
    }

    if (col_size == 0)
    {
        col_size = nullable_columns.begin()->second->size();
        for (size_t i = 0; i < col_size; ++i)
        {
            size_t numKVPairs = 0;
            for (auto it = nullable_columns.begin(); it != nullable_columns.end(); ++it)
            {
                if (!it->second->isNullAt(i))
                {
                    key_res_col.insert(Field(it->first));
                    if (is_lowcardinality_value)
                        value_res_col_lowcardinality->insertFromFullColumn(it->second->getNestedColumn(), i);
                    else
                        value_res_col.insertFrom(it->second->getNestedColumn(), i);
                    numKVPairs++;
                }
            }

            offset_res_col.push_back((offset_res_col.size() == 0 ? 0 : offset_res_col.back()) + numKVPairs);
        }
    }
    else
    {
        if (col_size != nullable_columns.begin()->second->size())
            throw Exception("Expect equal size between map and implicit columns", ErrorCodes::LOGICAL_ERROR);

        size_t offset = 0;
        size_t row_size = offsets_[0];

        for (size_t i = 0; i < col_size; ++i)
        {
            offset = offsets_[i - 1];
            row_size = offsets_[i] - offset;

            size_t numKVPairs = 0;
            for (size_t r = 0; r < row_size; ++r)
            {
                auto tmp_key = keyColumn->getDataAt(offset + r).toString();
                if (!nullable_columns.count(tmp_key))
                {
                    key_res_col.insertFrom(*keyColumn, offset + r);
                    value_res_col.insertFrom(*valueColumn, offset + r);
                    numKVPairs++;
                }
            }

            for (auto it = nullable_columns.begin(); it != nullable_columns.end(); ++it)
            {
                if (!it->second->isNullAt(i))
                {
                    key_res_col.insert(Field(it->first));
                    if (is_lowcardinality_value)
                        value_res_col_lowcardinality->insertFromFullColumn(it->second->getNestedColumn(), i);
                    else
                        value_res_col.insertFrom(it->second->getNestedColumn(), i);
                    numKVPairs++;
                }
            }

            offset_res_col.push_back((offset_res_col.size() == 0 ? 0 : offset_res_col.back()) + numKVPairs);
        }
    }


    keyColumn = std::move(key_res);
    valueColumn = std::move(value_res);
    offsets = std::move(offset_res);
}

// this optimization mostly not work for Map type
bool ColumnByteMap::hasEqualValues() const
{
    return false;
}

void ColumnByteMap::updateWeakHash32(WeakHash32 &) const
{
    throw Exception("Map doesn't support updateWeakHash32", ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnByteMap::updateHashFast(SipHash &) const
{
    throw Exception("Map doesn't support updateHashFast", ErrorCodes::NOT_IMPLEMENTED);
}

ColumnPtr ColumnByteMap::selectDefault(const Field) const
{
    size_t row_num = size();
    auto res = ColumnVector<UInt8>::create(row_num, 1);
    IColumn::Filter & filter = res->getData();
    /// TODO: improve by SIMD
    for (size_t i = 0; i < row_num; ++i)
        filter[i] = sizeAt(i) == 0;
    return res;
}

bool ColumnByteMap::replaceRow(
    const PaddedPODArray<UInt32> & indexes,
    const IColumn & rhs,
    const PaddedPODArray<UInt32> & rhs_indexes,
    const Filter * /*is_default_filter*/,
    size_t current_pos,
    size_t & first_part_pos,
    size_t & second_part_pos,
    MutableColumnPtr & res) const
{
    const ColumnByteMap & rhs_map_column = static_cast<const ColumnByteMap &>(rhs);
    RowValue row;
    mergeRowValue(row, current_pos);
    while (first_part_pos < indexes.size() && indexes[first_part_pos] == current_pos)
    {
        if (rhs_indexes[first_part_pos] >= size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of this column size {}", rhs_indexes[first_part_pos], size());
        mergeRowValue(row, rhs_indexes[first_part_pos]);
        first_part_pos++;
    }
    if (second_part_pos < indexes.size() && indexes[second_part_pos] - size() == current_pos)
    {
        if (rhs_indexes[second_part_pos] >= rhs.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of rhs column size {}", rhs_indexes[second_part_pos], rhs.size());
        rhs_map_column.mergeRowValue(row, rhs_indexes[second_part_pos]);
    }
    ByteMap row_field;
    row_field.reserve(row.size());
    for (auto & [key, val] : row)
        row_field.emplace_back(key, val);
    res->insert(row_field);
    return true;
}

void ColumnByteMap::mergeRowValue(RowValue & row, size_t n) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);

    for (size_t i = 0; i < size; ++i)
    {
        Field key = getKey()[offset + i];
        if (!row.count(key))
            row[key] = getValue()[offset + i];
    }
}
}
