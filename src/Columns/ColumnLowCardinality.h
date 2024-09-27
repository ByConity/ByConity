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
#include <Columns/IColumn.h>
#include <Columns/IColumnUnique.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include "ColumnsNumber.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * How data is stored (in a nutshell):
 * we have a dictionary @e reverse_index in ColumnUnique that holds pairs (DataType, UIntXX) and a column
 * with UIntXX holding actual data indices.
 * To obtain the value's index, call #getOrFindIndex.
 * To operate on the data (so called indices column), call #getIndexes.
 *
 * @note The indices column always contains the default value (empty StringRef) with the first index.
 */
class ColumnLowCardinality final : public COWHelper<IColumn, ColumnLowCardinality>
{
    friend class COWHelper<IColumn, ColumnLowCardinality>;

    ColumnLowCardinality(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, bool is_shared = false);
    ColumnLowCardinality(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, MutableColumnPtr && nest_column); // for full state

    ColumnLowCardinality(const ColumnLowCardinality & other) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnLowCardinality>;
    static Ptr create(const ColumnPtr & column_unique_, const ColumnPtr & indexes_, bool is_shared = false)
    {
        return ColumnLowCardinality::create(column_unique_->assumeMutable(), indexes_->assumeMutable(), is_shared);
    }

    static MutablePtr create(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, bool is_shared = false)
    {
        return Base::create(std::move(column_unique), std::move(indexes), is_shared);
    }

    static Ptr create(const ColumnPtr & column_unique_, const ColumnPtr & indexes_, const ColumnPtr & nest_column_)
    {
        return ColumnLowCardinality::create(column_unique_->assumeMutable(), indexes_->assumeMutable(), nest_column_->assumeMutable());
    }

    static MutablePtr create(MutableColumnPtr && column_unique, MutableColumnPtr && indexes, MutableColumnPtr && nest_column_)
    {
        return Base::create(std::move(column_unique), std::move(indexes), std::move(nest_column_));
    }

    std::string getName() const override { return "ColumnLowCardinality"; }
    const char * getFamilyName() const override { return "ColumnLowCardinality"; }
    TypeIndex getDataType() const override { return TypeIndex::LowCardinality; }

    ColumnPtr convertToFullColumn() const;
    ColumnPtr convertToFullColumnIfLowCardinality() const override { return convertToFullColumn(); }

    void tryToFlushZeroCopyBuffer() const override
    {

        if (idx.getPositions())
            idx.getPositions()->tryToFlushZeroCopyBuffer();
        if (nested_column)
            nested_column->tryToFlushZeroCopyBuffer();
        if (dictionary.getColumnUniquePtr()) {
            dictionary.getColumnUniquePtr()->tryToFlushZeroCopyBuffer();
        }
    }

    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override
    {
        if (full_state)
            return getNestedColumn().size();
        else
            return getIndexes().size();
    }

    Field operator[](size_t n) const override
    {
        if (full_state)
            return getNestedColumn()[n];
        else
            return getDictionary()[getIndexes().getUInt(n)];
    }

    void get(size_t n, Field & res) const override
    {
        if (full_state)
            getNestedColumn().get(n, res);
        else
            getDictionary().get(getIndexes().getUInt(n), res);
    }

    StringRef getDataAt(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getDataAt(n);
        else
            return getDictionary().getDataAt(getIndexes().getUInt(n));
    }

    bool isDefaultAt(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().isDefaultAt(n);
        else
            return getDictionary().isDefaultAt(getIndexes().getUInt(n));
    }

    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getDataAtWithTerminatingZero(n);
        else
            return getDictionary().getDataAtWithTerminatingZero(getIndexes().getUInt(n));
    }

    UInt64 get64(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().get64(n);
        else
            return getDictionary().get64(getIndexes().getUInt(n));
    }
    UInt64 getUInt(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getUInt(n);
        else
            return getDictionary().getUInt(getIndexes().getUInt(n));
    }
    Int64 getInt(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getInt(n);
        else
            return getDictionary().getInt(getIndexes().getUInt(n));
    }
    bool isNullAt(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().isNullAt(n);
        else
            return getDictionary().isNullAt(getIndexes().getUInt(n));
    }

    Float64 getFloat64(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getFloat64(n);
        else
            return getDictionary().getInt(getIndexes().getFloat64(n));
    }

    Float32 getFloat32(size_t n) const override
    {
        if (full_state)
            return  getNestedColumn().getFloat32(n);
        else
            return getDictionary().getInt(getIndexes().getFloat32(n));
    }

    bool getBool(size_t n) const override
    {
        if (full_state)
            return getNestedColumn().getBool(n);
        else
            return getDictionary().getInt(getIndexes().getBool(n));
    }

    ColumnPtr cut(size_t start, size_t length) const override
    {
        if (full_state)
        {
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr()->cloneEmpty(),
                                                getIndexes().cloneEmpty(), getNestedColumn().cut(start, length));
        }
        else
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().cut(start, length));
    }

    void insert(const Field & x) override;
    void insertDefault() override;

    void mergeGatherColumn(const IColumn &src, std::unordered_map<UInt64, UInt64> &reverseIndex);
    void loadDictionaryFrom(const IColumn & src);
    void insertIndexFrom(const IColumn & src, size_t n);
    void insertIndexRangeFrom(const IColumn & src, size_t start, size_t length);

    void insertFrom(const IColumn & src, size_t n) override;
    void insertFromFullColumn(const IColumn & src, size_t n);

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length);
    void insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions);
    void insertRangeSelective(const IColumn & src, const IColumn::Selector & selector, size_t selector_start, size_t length) override;

    void insertData(const char * pos, size_t length) override;

    void popBack(size_t n) override
    {
        if (isFullState())
            getNestedColumn().popBack(n);
        else
            idx.popBack(n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        if (full_state)
            return getNestedColumn().updateHashWithValue(n, hash);
        else
            return getDictionary().updateHashWithValue(getIndexes().getUInt(n), hash);
    }

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash &) const override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        if (full_state)
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr()->cloneEmpty(),
                                                 getIndexes().cloneEmpty(), getNestedColumn().filter(filt, result_size_hint));
        else
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().filter(filt, result_size_hint));
    }

    void expand(const Filter & mask, bool inverted) override
    {
        idx.getPositionsPtr()->expand(mask, inverted);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        if (full_state)
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr()->cloneEmpty(),
                                                getIndexes().cloneEmpty(), getNestedColumn().permute(perm, limit));
        else
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().permute(perm, limit));
    }

    ColumnPtr index(const IColumn & indexes_, size_t limit) const override
    {
        if (full_state)
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr()->cloneEmpty(),
                                                getIndexes().cloneEmpty(), getNestedColumn().index(indexes_, limit));
        else
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().index(indexes_, limit));
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator &) const override;

    bool hasEqualValues() const override;

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;

    void getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges& equal_ranges) const override;

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        if (full_state)
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr()->cloneEmpty(),
                                                getIndexes().cloneEmpty(), getNestedColumn().replicate(offsets));
        else
            return ColumnLowCardinality::create(dictionary.getColumnUniquePtr(), getIndexes().replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    void getExtremes(Field & min, Field & max) const override
    {
        if (full_state)
            return getNestedColumn().getExtremes(min, max);
        else
            return dictionary.getColumnUnique().getNestedColumn()->index(getIndexes(), 0)->getExtremes(min, max); /// TODO: optimize
    }

    void reserve(size_t n) override { if (full_state) getNestedColumn().reserve(n); else idx.reserve(n); }

    size_t byteSizeAt(size_t n) const override { return getDictionary().byteSizeAt(getIndexes().getUInt(n)); }
    size_t byteSize() const override
    {
        if (full_state)
            return getNestedColumn().byteSize();
        else
            return idx.getPositions()->byteSize() + getDictionary().byteSize();
    }

    size_t allocatedBytes() const override
    {
        if (full_state)
            return getNestedColumn().allocatedBytes();
        else
            return idx.getPositions()->allocatedBytes() + getDictionary().allocatedBytes();
    }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        if (full_state)
        {
            callback(nested_column);
            return;
        }

        callback(idx.getPositionsPtr());

        /// Column doesn't own dictionary if it's shared.
        if (!dictionary.isShared())
            callback(dictionary.getColumnUniquePtr());
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {

        if (isFullState())
        {
            callback(*nested_column);
            nested_column->forEachSubcolumnRecursively(callback);
        }

        callback(*idx.getPositionsPtr());
        idx.getPositionsPtr()->forEachSubcolumnRecursively(callback);

        /// Column doesn't own dictionary if it's shared.
        if (!dictionary.isShared())
        {
            callback(*dictionary.getColumnUniquePtr());
            dictionary.getColumnUniquePtr()->forEachSubcolumnRecursively(callback);
        }
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (full_state)
        {
            return getNestedColumn().structureEquals(rhs);
        }

        if (auto rhs_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&rhs))
            return idx.getPositions()->structureEquals(*rhs_low_cardinality->idx.getPositions())
                && dictionary.getColumnUnique().structureEquals(rhs_low_cardinality->dictionary.getColumnUnique());
        return false;
    }

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return getIndexes().getRatioOfDefaultRows(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return getIndexes().getNumberOfDefaultRows();
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        return getIndexes().getIndicesOfNonDefaultRows(indices, from, limit);
    }

    bool valuesHaveFixedSize() const override
    {
        if (full_state)
            return getNestedColumn().valuesHaveFixedSize();
        else
            return getDictionary().valuesHaveFixedSize();
    }
    bool isFixedAndContiguous() const override { if (full_state) return getNestedColumn().isFixedAndContiguous(); else return false; }
    size_t sizeOfValueIfFixed() const override { if (full_state) return getNestedColumn().sizeOfValueIfFixed(); else return getDictionary().sizeOfValueIfFixed(); }
    bool isNumeric() const override { return getDictionary().isNumeric(); }
    bool lowCardinality() const override { return true; }
    bool isCollationSupported() const override { return getDictionary().getNestedColumn()->isCollationSupported(); }

    /**
     * Checks if the dictionary column is Nullable(T).
     * So LC(Nullable(T)) would return true, LC(U) -- false.
     */
    bool nestedIsNullable() const { return isColumnNullable(*dictionary.getColumnUnique().getNestedColumn()); }
    void nestedToNullable() { dictionary.getColumnUnique().nestedToNullable(); }
    void nestedRemoveNullable() { dictionary.getColumnUnique().nestedRemoveNullable(); }
    MutableColumnPtr cloneNullable() const;

    const IColumnUnique & getDictionary() const { return dictionary.getColumnUnique(); }
    IColumnUnique & getDictionary() { return dictionary.getColumnUnique(); }
    const ColumnPtr & getDictionaryPtr() const { return dictionary.getColumnUniquePtr(); }
    /// IColumnUnique & getUnique() { return static_cast<IColumnUnique &>(*column_unique); }
    /// ColumnPtr getUniquePtr() const { return column_unique; }

    /// IColumn & getIndexes() { return *idx.getPositions(); }
    const IColumn & getIndexes() const { return *idx.getPositions(); }
    const ColumnPtr & getIndexesPtr() const { return idx.getPositions(); }
    size_t getSizeOfIndexType() const { return idx.getSizeOfIndexType(); }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        const IColumn * indexes = &getIndexes();

        switch (idx.getSizeOfIndexType())
        {
            case sizeof(UInt8): return assert_cast<const ColumnUInt8 *>(indexes)->getElement(row);
            case sizeof(UInt16): return assert_cast<const ColumnUInt16 *>(indexes)->getElement(row);
            case sizeof(UInt32): return assert_cast<const ColumnUInt32 *>(indexes)->getElement(row);
            case sizeof(UInt64): return assert_cast<const ColumnUInt64 *>(indexes)->getElement(row);
            default: throw Exception("Unexpected size of index type for low cardinality column.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    ///void setIndexes(MutableColumnPtr && indexes_) { indexes = std::move(indexes_); }

    /// Set shared ColumnUnique for empty low cardinality column.
    void setSharedDictionary(const ColumnPtr & column_unique);
    bool isSharedDictionary() const { return dictionary.isShared(); }

    /// Create column with new dictionary from column part.
    /// Dictionary will have only keys that are mentioned in index.
    MutablePtr cutAndCompact(size_t start, size_t length) const;

    struct DictionaryEncodedColumn
    {
        ColumnPtr dictionary;
        ColumnPtr indexes;
    };

    DictionaryEncodedColumn getMinimalDictionaryEncodedColumn(UInt64 offset, UInt64 limit) const;

    ColumnPtr countKeys() const;
    bool containsNull() const;
    void transformIndex(std::unordered_map<UInt64, UInt64>& trans, size_t max_size);

    bool isFullState() const { return full_state; }
    void setFullState(bool fullState) { full_state = fullState; }
    IColumn & getNestedColumn() { return *nested_column; }
    const IColumn & getNestedColumn() const { return *nested_column; }

    const ColumnPtr & getNestedColumnPtr() const { return nested_column; }
    ColumnPtr & getNestedColumnPtr() { return nested_column; }
    bool nestedCanBeInsideNullable() const { return dictionary.getColumnUnique().getNestedColumn()->canBeInsideNullable(); }
    ColumnPtr cloneWithDefaultOnNull() const;

    void switchToFull()
    {
        if (full_state)
            throw Exception("Unexpected switch to full.", ErrorCodes::LOGICAL_ERROR);

        nested_column = getDictionary().getNestedColumn()->index(getIndexes(), 0);
        idx = Index();
        dictionary = Dictionary(dictionary.getColumnUnique().cloneEmpty(), false);
        full_state = true;
    }

    void switchToFullWithDict(const ColumnPtr & dict)
    {
        if (full_state)
            throw Exception("Unexpected switch to full.", ErrorCodes::LOGICAL_ERROR);

        nested_column = dict->index(getIndexes(), 0);
        idx = Index();
        dictionary = Dictionary(dictionary.getColumnUnique().cloneEmpty(), false);
        full_state = true;
    }

    class Index
    {
    public:
        Index();
        Index(const Index & other) = default;
        Index &operator=(const Index &other) = default;

        explicit Index(MutableColumnPtr && positions_);
        explicit Index(ColumnPtr positions_);

        const ColumnPtr & getPositions() const { return positions; }
        WrappedPtr & getPositionsPtr() { return positions; }
        const WrappedPtr & getPositionsPtr() const { return positions; }
        size_t getPositionAt(size_t row) const;
        void insertPosition(UInt64 position);
        void insertPositionsRange(const IColumn & column, UInt64 offset, UInt64 limit);

        void popBack(size_t n) { positions->popBack(n); }
        void reserve(size_t n) { positions->reserve(n); }

        UInt64 getMaxPositionForCurrentType() const;

        static size_t getSizeOfIndexType(const IColumn & column, size_t hint);
        size_t getSizeOfIndexType() const { return size_of_type; }

        void checkSizeOfType();

        ColumnPtr detachPositions() { return std::move(positions); }
        void attachPositions(ColumnPtr positions_);

        void countKeys(ColumnUInt64::Container & counts) const;
        bool containsDefault() const;

        void updateWeakHash(WeakHash32 & hash, WeakHash32 & dict_hash) const;
        void transformIndex(std::unordered_map<UInt64, UInt64>& trans, size_t max_size);
    private:
        WrappedPtr positions;
        size_t size_of_type = 0;

        void updateSizeOfType() { size_of_type = getSizeOfIndexType(*positions, size_of_type); }
        void expandType();

        template <typename IndexType>
        typename ColumnVector<IndexType>::Container & getPositionsData();

        template <typename IndexType>
        const typename ColumnVector<IndexType>::Container & getPositionsData() const;

        template <typename IndexType>
        void convertPositions();

        template <typename Callback>
        static void callForType(Callback && callback, size_t size_of_type);
    };

private:
    class Dictionary
    {
    public:
        Dictionary(const Dictionary & other) = default;
        Dictionary &operator=(const Dictionary &other) = default;

        explicit Dictionary(MutableColumnPtr && column_unique, bool is_shared);
        explicit Dictionary(ColumnPtr column_unique, bool is_shared);

        const ColumnPtr & getColumnUniquePtr() const { return column_unique; }
        WrappedPtr & getColumnUniquePtr() { return column_unique; }

        const IColumnUnique & getColumnUnique() const { return static_cast<const IColumnUnique &>(*column_unique); }
        IColumnUnique & getColumnUnique() { return static_cast<IColumnUnique &>(*column_unique); }

        /// Dictionary may be shared for several mutable columns.
        /// Immutable columns may have the same column unique, which isn't necessarily shared dictionary.
        void setShared(const ColumnPtr & column_unique_);
        bool isShared() const { return shared; }

        /// Create new dictionary with only keys that are mentioned in positions.
        void compact(ColumnPtr & positions);

    private:
        WrappedPtr column_unique;
        bool shared = false;
    };

    Dictionary dictionary;
    Index idx;
    WrappedPtr nested_column;
    bool full_state = false;

    void compactInplace();
    void compactIfSharedDictionary();

    int compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator=nullptr) const;

    void getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator = nullptr) const;
};

bool isColumnLowCardinalityNullable(const IColumn & column);


}
