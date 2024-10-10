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

#include <Columns/ColumnMap.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumnImpl.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <common/map.h>
#include <common/range.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/WeakHash.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    ColumnPtr tryMakeNullableForMapValue(const ColumnPtr & column)
    {
        if (column->isNullable())
            return column;
        /// When column is low cardinality, its dictionary type must be nullable, see more detail in DataTypeLowCardinality::canBeByteMapValueType()
        if (const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column.get()))
        {
            if (low_cardinality_column->getDictionary().nestedColumnIsNullable())
                return column;
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Can not get map implicit column of lowcardinality column.");
        }
        else
        {
            if (column->canBeInsideNullable())
                return makeNullable(column);
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Can not get map implicit column of column {}.", column->getName());
        }
    }

    /**
     * Column could be in the following two mode:
     * 1. Column is low cardinality type and its dictionary type is nullable, see more detail in DataTypeLowCardinality::canBeByteMapValueType()
     * 2. Column is other type column, i.e. String, Numbers which can be inside nullable and need to be wrapped in Nullable again for querying implicit column.
     * For the first mode, we can use value column type directly when querying implicit column.
     */
    bool needAddNullable(const ColumnPtr & column) { return !column->lowCardinality(); }

    /**
     * Check if new column which will be inserted has obeyed the above rules.
     */
    void checkNewColumnCanBeInserted(const ColumnPtr & value_column, bool add_nullable, const IColumn & new_column)
    {
        if (add_nullable)
        {
            const auto * nullable_value_column = dynamic_cast<const ColumnNullable *>(&new_column);
            if (!nullable_value_column)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The new column can not be cast to nullable, its name is {}, map value column name is {}",
                    new_column.getName(),
                    value_column->getName());
            }
            if (nullable_value_column->getNestedColumnPtr()->getDataType() != value_column->getDataType())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The nested column type {} of nullable column is not equal to map value column type {}",
                    nullable_value_column->getNestedColumnPtr()->getName(),
                    value_column->getName());
            }
        }
        else if (new_column.getDataType() != value_column->getDataType())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The new column type {} is not equal to map value column type {}",
                new_column.getName(),
                value_column->getName());
        }
    }
}

std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    const auto & nested_tuple = getNestedData();
    res << "Map(" << nested_tuple.getColumn(0).getName()
        << ", " << nested_tuple.getColumn(1).getName() << ")";

    return res.str();
}

ColumnMap::ColumnMap(MutableColumnPtr && nested_)
    : nested(std::move(nested_))
{
    const auto * column_array = typeid_cast<const ColumnArray *>(nested.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap can be created only from array of tuples");

    const auto * column_tuple = typeid_cast<const ColumnTuple *>(column_array->getDataPtr().get());
    if (!column_tuple)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap can be created only from array of tuples");

    if (column_tuple->getColumns().size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMap should contain only 2 subcolumns: keys and values");

    for (const auto & column : column_tuple->getColumns())
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnMap cannot have ColumnConst as its element");
}

MutableColumnPtr ColumnMap::cloneEmpty() const
{
    return ColumnMap::create(nested->cloneEmpty());
}

MutableColumnPtr ColumnMap::cloneResized(size_t new_size) const
{
    return ColumnMap::create(nested->cloneResized(new_size));
}

Field ColumnMap::operator[](size_t n) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);
    Map res(size);

    for (size_t i=0; i<size; ++i)
    {
        res[i].first = getKey()[offset + i];
        res[i].second = getValue()[offset + i];
    }
    return res;
}

void ColumnMap::get(size_t n, Field & res) const
{
    size_t offset = offsetAt(n);
    size_t size = sizeAt(n);
    res = Map(size);
    Map & res_map = DB::get<Map &>(res);
    for (size_t i = 0; i < size; ++i)
    {
        getKey().get(offset + i, res_map[i].first);
        getValue().get(offset + i, res_map[i].second);
    }
}

bool ColumnMap::isDefaultAt(size_t n) const
{
    return nested->isDefaultAt(n);
}

StringRef ColumnMap::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insert(const Field & x)
{
    const Map & map = DB::get<const Map &>(x);
    size_t size = map.size();

    for (size_t i = 0; i < size; ++i)
    {
        getKey().insert(map[i].first);
        getValue().insert(map[i].second);
    }
    getOffsets().push_back(getOffsets().back() + size);
}

void ColumnMap::insertDefault()
{
    nested->insertDefault();
}
void ColumnMap::popBack(size_t n)
{
    nested->popBack(n);
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested->serializeValueIntoArena(n, arena, begin);
}

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    return nested->deserializeAndInsertFromArena(pos);
}

const char * ColumnMap::skipSerializedInArena(const char * pos) const
{
    return nested->skipSerializedInArena(pos);
}

void ColumnMap::updateHashWithValue(size_t n, SipHash & hash) const
{
    nested->updateHashWithValue(n, hash);
}

void ColumnMap::updateWeakHash32(WeakHash32 & hash) const
{
    nested->updateWeakHash32(hash);
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    nested->updateHashFast(hash);
}

void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    nested->insertRangeFrom(
        assert_cast<const ColumnMap &>(src).getNestedColumn(),
        start, length);
}

void ColumnMap::insertRangeSelective(const IColumn & src, const Selector & selector, size_t selector_start, size_t length)
{
    nested->insertRangeSelective(assert_cast<const ColumnMap &>(src).getNestedColumn(), selector, selector_start, length);
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    auto filtered = nested->filter(filt, result_size_hint);
    return ColumnMap::create(filtered);
}

void ColumnMap::expand(const IColumn::Filter & mask, bool inverted)
{
    nested->expand(mask, inverted);
}

ColumnPtr ColumnMap::permute(const Permutation & perm, size_t limit) const
{
    auto permuted = nested->permute(perm, limit);
    return ColumnMap::create(std::move(permuted));
}

ColumnPtr ColumnMap::index(const IColumn & indexes, size_t limit) const
{
    auto res = nested->index(indexes, limit);
    return ColumnMap::create(std::move(res));
}

ColumnPtr ColumnMap::replicate(const Offsets & offsets_) const
{
    auto replicated = nested->replicate(offsets_);
    return ColumnMap::create(std::move(replicated));
}

MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto scattered_columns = nested->scatter(num_columns, selector);
    MutableColumns res;
    res.reserve(num_columns);
    for (auto && scattered : scattered_columns)
        res.push_back(ColumnMap::create(std::move(scattered)));

    return res;
}

int ColumnMap::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto & rhs_map = assert_cast<const ColumnMap &>(rhs);
    return nested->compareAt(n, m, rhs_map.getNestedColumn(), nan_direction_hint);
}

void ColumnMap::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnMap>(assert_cast<const ColumnMap &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

bool ColumnMap::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnMap>();
}

void ColumnMap::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    nested->getPermutation(direction, stability, limit, nan_direction_hint, res);
}

void ColumnMap::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    nested->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
}

void ColumnMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnMap::reserve(size_t n)
{
    nested->reserve(n);
}

size_t ColumnMap::byteSize() const
{
    return nested->byteSize();
}

size_t ColumnMap::byteSizeAt(size_t n) const
{
    return nested->byteSizeAt(n);
}

size_t ColumnMap::allocatedBytes() const
{
    return nested->allocatedBytes();
}

void ColumnMap::protect()
{
    nested->protect();
}

void ColumnMap::getExtremes(Field & min, Field & max) const
{
    Field nested_min;
    Field nested_max;

    nested->getExtremes(nested_min, nested_max);

    /// Convert result Array fields to Map fields because client expect min and max field to have type Map

    Array nested_min_value = nested_min.get<Array>();
    Array nested_max_value = nested_max.get<Array>();

    Map map_min_value;
    Map map_max_value;
    map_min_value.reserve(nested_min_value.size());
    map_max_value.reserve(nested_max_value.size());

    for (const auto & elem : nested_min_value)
    {
        const auto & tuple = elem.safeGet<const Tuple &>();
        assert(tuple.size() == 2);
        map_min_value.emplace_back(tuple[0], tuple[1]);
    }

    for (const auto & elem : nested_max_value)
    {
        const auto & tuple = elem.safeGet<const Tuple &>();
        assert(tuple.size() == 2);
        map_max_value.emplace_back(tuple[0], tuple[1]);
    }

    min = std::move(map_min_value);
    max = std::move(map_max_value);
}

void ColumnMap::forEachSubcolumn(MutableColumnCallback callback)
{
    nested->forEachSubcolumn(callback);
}

void ColumnMap::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*nested);
    nested->forEachSubcolumnRecursively(callback);
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return nested->structureEquals(*rhs_map->nested);
    return false;
}

ColumnPtr ColumnMap::compress() const
{
    auto compressed = nested->compress();
    return ColumnCompressed::create(size(), compressed->byteSize(), [compressed = std::move(compressed)]
    {
        return ColumnMap::create(compressed->decompress());
    });
}

bool ColumnMap::replaceRow(
    const PaddedPODArray<UInt32> & indexes,
    const IColumn & rhs,
    const PaddedPODArray<UInt32> & rhs_indexes,
    const Filter * is_default_filter,
    size_t current_pos,
    size_t & first_part_pos,
    size_t & second_part_pos,
    MutableColumnPtr & res,
    bool enable_merge_map) const
{
    if (!enable_merge_map)
        return IColumn::replaceRow(indexes, rhs, rhs_indexes, is_default_filter, current_pos, first_part_pos, second_part_pos, res, enable_merge_map);

    const auto & rhs_map_column = static_cast<const ColumnMap &>(rhs);
    RowValue row;
    if (is_default_filter && !(*is_default_filter)[current_pos])
        mergeRowValue(row, current_pos);
    while (first_part_pos < indexes.size() && indexes[first_part_pos] == current_pos)
    {
        if (rhs_indexes[first_part_pos] >= size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "rhs_index {} is out of this column size {}", rhs_indexes[first_part_pos], size());
        if (is_default_filter && !(*is_default_filter)[rhs_indexes[first_part_pos]])
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
    Map row_field;
    row_field.reserve(row.size());
    for (auto & [key, val] : row)
        row_field.emplace_back(key, val);
    res->insert(row_field);
    return true;
}

void ColumnMap::mergeRowValue(RowValue & row, size_t n) const
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

double ColumnMap::getRatioOfDefaultRows(double sample_ratio) const
{
    return getRatioOfDefaultRowsImpl<ColumnMap>(sample_ratio);
}

UInt64 ColumnMap::getNumberOfDefaultRows() const
{
    return getNumberOfDefaultRowsImpl<ColumnMap>();
}

void ColumnMap::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    return getIndicesOfNonDefaultRowsImpl<ColumnMap>(indices, from, limit);
}

/**
 * Generic implementation of get implicit value column based on key value.
 * TODO: specialize this function for Number type and String type.
 */
ColumnPtr ColumnMap::getValueColumnByKey(const StringRef & key, size_t rows_to_read) const
{
    size_t col_size = size();

    const auto & key_column = getKeyPtr();
    const auto & value_column = getValuePtr();
    const auto & offsets_ = getOffsets();
    /// Mark whether res has been wrapped nullable explicitly.
    bool add_nullable = needAddNullable(value_column);
    ColumnPtr res = createEmptyImplicitColumn();

    if (col_size == 0)
        return res;
    auto & res_col = res->assumeMutableRef();
    res_col.reserve(col_size);
    size_t offset = 0;

    size_t row_size = offsets_[0];
    bool found_key = false;

    size_t rows = rows_to_read == 0 ? col_size : std::min(rows_to_read, col_size);
    for (size_t i = 0; i < rows; ++i)
    {
        found_key = false;
        offset = offsets_[i - 1];
        row_size = offsets_[i] - offset;

        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = key_column->getDataAt(offset + r);
            if (tmp_key == key)
            {
                found_key = true;
                /// Handle lowcardinality type
                if (!add_nullable)
                    res_col.insertFrom(*value_column, offset + r);
                else
                {
                    static_cast<ColumnNullable &>(res_col).getNestedColumn().insertFrom(*value_column, offset + r);
                    static_cast<ColumnNullable &>(res_col).getNullMapData().push_back(0);
                }
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

void ColumnMap::constructAllImplicitColumns(
    std::unordered_map<StringRef, String> & key_name_map, std::unordered_map<StringRef, ColumnPtr> & value_columns) const
{
    /// This is an one-pass algorithm, to minimize the cost of traversing all key-value pairs:
    /// 1) Names of key and implicit value columns are created lazily;
    /// 2) NULLs are filled as need before non-null values inserted or in the end.

    const auto & key_column = getKeyPtr();
    const auto & value_column = getValuePtr();
    const auto & offsets_ = getOffsets();
    /// Mark whether res has been wrapped nullable explicitly
    bool add_nullable = needAddNullable(value_column);
    ColumnPtr empty_implicit_column = createEmptyImplicitColumn();
    size_t col_size = size();

    for (size_t r = 0; r < col_size; ++r)
    {
        const size_t offset = offsets_[r - 1]; /// -1th index is Ok, see PaddedPODArray
        const size_t curr_num_pair = offsets_[r] - offset;

        for (size_t p = 0; p < curr_num_pair; ++p)
        {
            auto tmp_key = key_column->getDataAt(offset + p);
            auto iter = value_columns.find(tmp_key);

            if (iter == value_columns.end())
            {
                key_name_map[tmp_key] = applyVisitor(DB::FieldVisitorToString(), (*key_column)[offset + p]);
                ColumnPtr new_column = empty_implicit_column->cloneEmpty();
                new_column->assumeMutableRef().reserve(col_size);

                iter = value_columns.try_emplace(tmp_key, new_column).first;
            }

            auto & impl_value_column = iter->second->assumeMutableRef();
            /// Fill NULLs as need
            while (impl_value_column.size() < r)
                impl_value_column.insert(Null());

            /// Handle duplicated keys in map
            if (likely(impl_value_column.size() == r))
            {
                if (!add_nullable)
                    impl_value_column.insertFrom(*value_column, offset + p);
                else
                {
                    auto & nullable_impl_value_column = typeid_cast<ColumnNullable &>(impl_value_column);
                    nullable_impl_value_column.getNestedColumn().insertFrom(*value_column, offset + p);
                    nullable_impl_value_column.getNullMapData().push_back(0);
                }
            }
        }
    }

    /// Fill NULLs until all columns reach the same size
    for (auto & [k, column] : value_columns)
    {
        auto & impl_value_column = column->assumeMutableRef();
        while (impl_value_column.size() < col_size)
            impl_value_column.insert(Null());
    }
}

/**
 * This routine will reconsturct MAP column based on its implicit columns.
 * \param impl_key_values is the collection of {key_name, {offset, key_column}}
 * and this routine will be compatible with non-zero offset. key_name has been escaped.
 * non-zero offset could happen for scenario that map column and implicit column are both
 * referenced in the same query, e.g.
 *                  select map from table where map{'key'} =1
 * In the above case, MergeTreeReader will use the same stream __map__%27key%27 for
 * both keys, while reconstructing(append) map column, size of implicit column
 * __map__%27key%27 was accumulated.
 */
void ColumnMap::fillByExpandedColumns(
    const DataTypeMap & map_type, const std::map<String, std::pair<size_t, const IColumn *>> & impl_key_values, size_t rows)
{
    // Append to ends of this ColumnMap
    if (impl_key_values.empty())
    {
        // Insert default values
        insertManyDefaults(rows);
        return;
    }

    for (auto it = impl_key_values.begin(); it != impl_key_values.end(); ++it)
    {
        if (it->second.first + rows != it->second.second->size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "MAP implicit key size is not match required rows, key: {}, offset: {}, size: {}, required rows: {}",
                it->first,
                it->second.first,
                it->second.second->size(),
                rows);
    }

    IColumn & key_col = getKey();
    IColumn & value_col = getValue();
    Offsets & offsets_ = getOffsets();
    size_t all_keys_num = impl_key_values.size();
    std::vector<Field> keys;
    keys.reserve(all_keys_num);

    auto & key_type = map_type.getKeyType();

    //Intepreter key columns
    for (auto & kv : impl_key_values)
    {
        keys.push_back(key_type->stringToVisitorField(kv.first));
        // Sanity check that all implicit columns(plus offset) are the same size
        if (rows + kv.second.first != kv.second.second->size())
        {
            throw Exception(
                "implicit column is with different size " + toString(kv.second.second->size()) + " " + toString(rows + kv.second.first),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    std::vector<const IColumn *> nested_columns;
    /// Mark whether res has been wrapped nullable explicitly in order to use insertFrom method instead of insert and [] method to improve performance.
    bool add_nullable = needAddNullable(getValuePtr());
    /// Check type of input columns and prepare nest columns for the columns whose type is neither nullable or low cardinality.
    for (auto it = impl_key_values.begin(); it != impl_key_values.end(); ++it)
    {
        checkNewColumnCanBeInserted(getValuePtr(), add_nullable, *it->second.second);
        if (add_nullable)
        {
            const auto & inner_value_column = typeid_cast<const ColumnNullable &>(*it->second.second);
            nested_columns.emplace_back(&inner_value_column.getNestedColumn());
        }
    }
    for (size_t i = 0; i < rows; i++)
    {
        size_t num_kv_pairs = 0;
        size_t iter_idx = 0;
        for (auto it = impl_key_values.begin(); it != impl_key_values.end(); ++it, ++iter_idx)
        {
            size_t impl_offset = it->second.first;
            const IColumn & impl_value_col = *it->second.second;

            // Ignore those input whose value is NULL.
            if (impl_value_col.isNullAt(i + impl_offset))
                continue;

            key_col.insert(keys[iter_idx]);
            if (add_nullable)
                value_col.insertFrom(*nested_columns[iter_idx], i + impl_offset);
            else
                value_col.insertFrom(impl_value_col, i + impl_offset);
            num_kv_pairs++;
        }
        offsets_.push_back((offsets_.size() == 0 ? 0 : offsets_.back()) + num_kv_pairs);
    }
}

void ColumnMap::removeKeys(const NameSet & keys)
{
    size_t col_size = size();
    if (col_size == 0)
        return;

    const auto & key_column = getKeyPtr();
    const auto & value_column = getValuePtr();
    const auto & offsets = getOffsetsPtr();
    const Offsets & offsets_ = getOffsets();

    MutableColumnPtr key_res = key_column->cloneEmpty();
    MutableColumnPtr value_res = value_column->cloneEmpty();
    MutableColumnPtr offset_res = offsets->cloneEmpty();
    Offsets & offset_res_data = typeid_cast<ColumnOffsets &>(offset_res->assumeMutableRef()).getData();

    size_t offset = 0;
    size_t row_size = offsets_[0];

    for (size_t i = 0; i < col_size; ++i)
    {
        offset = offsets_[i - 1];
        row_size = offsets_[i] - offset;

        size_t num_kv_pairs = 0;
        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = key_column->getDataAt(offset + r).toString();
            if (!keys.count(tmp_key))
            {
                key_res->insertFrom(*key_column, offset + r);
                value_res->insertFrom(*value_column, offset + r);
                num_kv_pairs++;
            }
        }
        offset_res_data.push_back((offset_res_data.size() == 0 ? 0 : offset_res_data.back()) + num_kv_pairs);
    }

    nested = ColumnArray::create(ColumnTuple::create(Columns{std::move(key_res), std::move(value_res)}), std::move(offset_res));
}

/**
 * Insert implicit map column from outside, only when the current ColumnMap has the same size with implicit columns.
 * Currently, this method works for ingestion column feature.
 *
 * @param implicit_columns it should has the same type with return value of ColumnMap::getValueColumnByKey. Otherwise, throw exception.
 */
void ColumnMap::insertImplicitMapColumns(const std::unordered_map<String, ColumnPtr> & implicit_columns)
{
    if (implicit_columns.empty())
        return;

    const auto & key_column = getKeyPtr();
    const auto & value_column = getValuePtr();
    const auto & offsets = getOffsetsPtr();
    const Offsets & offsets_ = getOffsets();

    MutableColumnPtr key_res = key_column->cloneEmpty();
    MutableColumnPtr value_res = value_column->cloneEmpty();
    MutableColumnPtr offset_res = offsets->cloneEmpty();
    Offsets & offset_res_data = typeid_cast<ColumnOffsets &>(offset_res->assumeMutableRef()).getData();

    /// Mark whether res has been wrapped nullable explicitly.
    bool add_nullable = needAddNullable(value_column);
    std::vector<const IColumn *> nested_columns;
    /// Check type of input columns and prepare nested columns for the columns whose type is neither nullable or low cardinality.
    for (auto it = implicit_columns.begin(); it != implicit_columns.end(); ++it)
    {
        checkNewColumnCanBeInserted(value_column, add_nullable, *it->second);
        if (add_nullable)
        {
            const auto & inner_value_column = typeid_cast<const ColumnNullable &>(*it->second);
            nested_columns.emplace_back(&inner_value_column.getNestedColumn());
        }
    }

    size_t origin_size = size();
    size_t add_size = implicit_columns.begin()->second->size();
    if (origin_size != 0 && origin_size != add_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Expect equal size between map and implicit columns, but got {}/{}", origin_size, add_size);

    size_t offset = 0;
    size_t row_size = offsets_[0];
    /// Handle each row data
    for (size_t i = 0; i < add_size; ++i)
    {
        /// Handle the case that origin column is empty
        offset = origin_size == 0 ? 0 : offsets_[i - 1];
        row_size = origin_size == 0 ? 0 : (offsets_[i] - offset);

        size_t num_kv_pairs = 0;
        for (size_t r = 0; r < row_size; ++r)
        {
            auto tmp_key = key_column->getDataAt(offset + r).toString();
            if (!implicit_columns.count(tmp_key))
            {
                key_res->insertFrom(*key_column, offset + r);
                value_res->insertFrom(*value_column, offset + r);
                num_kv_pairs++;
            }
        }

        size_t id = 0;
        for (auto it = implicit_columns.begin(); it != implicit_columns.end(); ++it, ++id)
        {
            /// we ignore null value due to it's meaningless.
            if (!it->second->isNullAt(i))
            {
                key_res->insert(Field(it->first));
                value_res->insertFrom(add_nullable ? *nested_columns[id]: *it->second, i);
                num_kv_pairs++;
            }
        }
        offset_res_data.push_back((offset_res_data.size() == 0 ? 0 : offset_res_data.back()) + num_kv_pairs);
    }

    nested = ColumnArray::create(ColumnTuple::create(Columns{std::move(key_res), std::move(value_res)}), std::move(offset_res));
}

ColumnPtr ColumnMap::createEmptyImplicitColumn() const
{
    return tryMakeNullableForMapValue(getValuePtr()->cloneEmpty());
}

}
