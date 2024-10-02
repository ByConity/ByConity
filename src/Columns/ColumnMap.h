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

#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMap.h>

namespace DB
{

/**
 * Column, that stores a nested Array(Tuple(key, value)) column.
 * Column, runtime ColumnMap is implicited stored as two implicit columns(key, value) and one offset column to count # k-v pair per row.
 */
class ColumnMap final : public COWHelper<IColumn, ColumnMap>
{
private:
    friend class COWHelper<IColumn, ColumnMap>;

    WrappedPtr nested;

    using RowValue = std::map<Field, Field>;

    explicit ColumnMap(MutableColumnPtr && nested_);

    ColumnMap(const ColumnMap &) = default;

    /**
     * For the feature of partial update, the row of map column will merge the values.
     * The caller must make sure that those Fields already in row is the newest one.
     *
     * @param row result of this row
     * @param n the row to be merged
     */
    void mergeRowValue(RowValue & row, size_t n) const;
    
public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;

    using ColumnOffsets = ColumnArray::ColumnOffsets;

    static Ptr create(const ColumnPtr & keys, const ColumnPtr & values, const ColumnPtr & offsets)
    {
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{keys, values}), offsets);
        return ColumnMap::create(nested_column);
    }

    static Ptr create(const ColumnPtr & column) { return ColumnMap::create(column->assumeMutable()); }
    static Ptr create(ColumnPtr && arg) { return create(arg); }

    template <typename ... Args>
    requires (IsMutableColumns<Args ...>::value)
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getDataType() const override { return TypeIndex::Map; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return nested->size(); }

    size_t ALWAYS_INLINE offsetAt(size_t i) const {return getOffsets()[i-1];}
    size_t ALWAYS_INLINE sizeAt(size_t i) const {return getOffsets()[i] - getOffsets()[i-1];}

    void tryToFlushZeroCopyBuffer() const override
    {

        if (nested)
            nested->tryToFlushZeroCopyBuffer();
    }

    /**
     * Try to replace the current value of current_pos with merging the value and those of two parts. The same target index will appear multiple times in first part and at most once in second part.
     * For example, there are three data of the same row(current_pos) from old to new:
     * {'a': 1, 'b': 2}, {'b': 3, 'c': 4}, {'a': 7,'c': 5, 'd': 6}
     * After merging data, result row is {'a': 7, 'b': 3, 'c': 5, 'd': 6}
     */
    bool replaceRow(
        const PaddedPODArray<UInt32> & indexes,
        const IColumn & rhs,
        const PaddedPODArray<UInt32> & rhs_indexes,
        const Filter * is_default_filter,
        size_t current_pos,
        size_t & first_part_pos,
        size_t & second_part_pos,
        MutableColumnPtr & res,
        bool enable_merge_map) const override;

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertRangeSelective(const IColumn & src, const Selector & selector, size_t selector_start, size_t length) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    bool hasEqualValues() const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(MutableColumnCallback callback) override;
    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;
    double getRatioOfDefaultRows(double sample_ratio) const override;
    UInt64 getNumberOfDefaultRows() const override;
    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override;

    /** Access embeded columns*/
    const ColumnArray & getNestedColumn() const { return assert_cast<const ColumnArray &>(*nested); }
    ColumnArray & getNestedColumn() { return assert_cast<ColumnArray &>(*nested); }

    const ColumnPtr & getNestedColumnPtr() const { return nested; }
    ColumnPtr & getNestedColumnPtr() { return nested; }

    const ColumnTuple & getNestedData() const { return assert_cast<const ColumnTuple &>(getNestedColumn().getData()); }
    ColumnTuple & getNestedData() { return assert_cast<ColumnTuple &>(getNestedColumn().getData()); }

    IColumn & getKey() { return getNestedData().getColumnPtr(0)->assumeMutableRef(); }
    const IColumn & getKey() const { return getNestedData().getColumn(0); }

    ColumnPtr & getKeyPtr() { return getNestedData().getColumnPtr(0); }
    const ColumnPtr & getKeyPtr() const { return getNestedData().getColumnPtr(0); }

    IColumn & getValue() { return getNestedData().getColumnPtr(1)->assumeMutableRef(); }
    const IColumn & getValue() const { return getNestedData().getColumn(1); }

    ColumnPtr & getValuePtr() { return getNestedData().getColumnPtr(1); }
    const ColumnPtr & getValuePtr() const { return getNestedData().getColumnPtr(1); }

    ColumnPtr & getOffsetsPtr() { return getNestedColumn().getOffsetsPtr(); }
    const ColumnPtr & getOffsetsPtr() const { return getNestedColumn().getOffsetsPtr(); }

    IColumn & getOffsetsColumn() { return getNestedColumn().getOffsetsPtr()->assumeMutableRef(); }
    const IColumn & getOffsetsColumn() const { return *getNestedColumn().getOffsetsPtr(); }

    Offsets & ALWAYS_INLINE getOffsets() { return static_cast<ColumnOffsets &>(getNestedColumn().getOffsetsPtr()->assumeMutableRef()).getData(); }

    const Offsets & ALWAYS_INLINE getOffsets() const { return static_cast<const ColumnOffsets &>(*getNestedColumn().getOffsetsPtr()).getData(); }

    /// For ByteMap
    /** Read limited implicit column for the given key. */
    ColumnPtr getValueColumnByKey(const StringRef & key, size_t rows_to_read = 0) const;

    /** Read all data and construct implicit columns. */
    void constructAllImplicitColumns(
        std::unordered_map<StringRef, String> & key_name_map, std::unordered_map<StringRef, ColumnPtr> & value_columns) const;

    /** This routine will reconsturct MAP column based on its implicit columns. */
    void fillByExpandedColumns(const DataTypeMap &, const std::map<String, std::pair<size_t, const IColumn *>> &, size_t row);

    /**
     * Remove data of map keys from the column.
     * Note: currently, this mothod is only used in the case that handling command of "clear map key" and the type of part is in-memory. In other on-disk part type(compact and wide), it can directly handle the files.
     */
    void removeKeys(const NameSet & keys);

    void insertImplicitMapColumns(const std::unordered_map<String, ColumnPtr> & implicit_columns);

    /** Make column nullable for implicit column */
    ColumnPtr createEmptyImplicitColumn() const;

    ColumnPtr compress() const override;
};

}
