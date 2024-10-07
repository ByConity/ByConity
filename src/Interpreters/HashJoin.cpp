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

#include <algorithm>
#include <any>
#include <atomic>
#include <limits>
#include <mutex>

#include "Common/Stopwatch.h"
#include <common/defines.h>
#include <common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/HashJoin.h>
#include <Interpreters/join_common.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/DictionaryReader.h>
#include <Interpreters/Context.h>

#include <Storages/StorageDictionary.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Core/ColumnNumbers.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/HashTable/HashSet.h>
#include "Columns/ColumnsCommon.h"
#include "Columns/FilterDescription.h"
#include "Columns/IColumn.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/DataTypesNumber.h"
#include "Interpreters/ExpressionActions.h"
#include "Interpreters/RowRefs.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include <Common/ProfileEvents.h>
#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event PerfInequalConditionElapsedMicroseconds;
    extern const Event PerfJoinElapsedMicroseconds;
    extern const Event PerfInequalConditionGetRowMicroseconds;
    extern const Event PerfInequalConditionExecuteMicroseconds;
    extern const Event PerfInequalConditionAppendMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
};

}

namespace JoinStuff
{
    /// Version of `getUsed` with dynamic dispatch
    bool JoinUsedFlags::getUsedSafe(size_t i) const
    {
        if (flags.empty())
            return !need_flags;
        return flags[i].load();
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    void JoinUsedFlags::reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS>::flagged)
        {
            assert(flags.size() <= size);
            need_flags = true;
            if (flags.empty() || flags.size() < size)
            {
                flags = VectorWithAlloc<std::atomic<bool>>(size);
            }
        }
    }

    template <>
    void JoinUsedFlags::setUsed<false>(size_t i [[maybe_unused]]) {}

    template <>
    bool JoinUsedFlags::getUsed<false>(size_t i [[maybe_unused]]) { return true; }

    template <>
    bool JoinUsedFlags::setUsedOnce<false>(size_t i [[maybe_unused]]) { return true; }

    template <>
    void JoinUsedFlags::setUsed<true>(size_t i)
    {
        /// Could be set simultaneously from different threads.
        flags[i].store(true, std::memory_order_relaxed);
    }

    template <>
    bool JoinUsedFlags::getUsed<true>(size_t i) { return flags[i].load(); }

    template <>
    bool JoinUsedFlags::setUsedOnce<true>(size_t i)
    {
        /// fast check to prevent heavy CAS with seq_cst order
        if (flags[i].load(std::memory_order_relaxed))
            return false;

        bool expected = false;
        return flags[i].compare_exchange_strong(expected, true);
    }
}

static ColumnPtr filterWithBlanks(ColumnPtr src_column, const IColumn::Filter & filter, bool inverse_filter = false)
{
    ColumnPtr column = src_column->convertToFullColumnIfConst();
    MutableColumnPtr mut_column = column->cloneEmpty();
    mut_column->reserve(column->size());

    if (inverse_filter)
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertDefault();
            else
                mut_column->insertFrom(*column, row);
        }
    }
    else
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertFrom(*column, row);
            else
                mut_column->insertDefault();
        }
    }

    return mut_column;
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column))
                column.column = filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }

    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
        if (column.type->isNullable() && !negative_null_map.empty())
        {
            MutableColumnPtr mutable_column = IColumn::mutate(std::move(column.column));
            assert_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    else
        JoinCommon::removeColumnNullability(column);

    return std::move(column);
}


template <ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, bool has_inequal_condition>
struct JoinFeatures;

/// Join features with inequal condition
template <ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness>
struct JoinFeatures<kind, strictness, true>
{
    static constexpr bool is_any_join = strictness == ASTTableJoin::Strictness::Any;
    static constexpr bool is_all_join = strictness == ASTTableJoin::Strictness::All;
    static constexpr bool is_asof_join = strictness == ASTTableJoin::Strictness::Asof;
    static constexpr bool is_semi_join = strictness == ASTTableJoin::Strictness::Semi;
    static constexpr bool is_anti_join = strictness == ASTTableJoin::Strictness::Anti;

    static constexpr bool left = kind == ASTTableJoin::Kind::Left;
    static constexpr bool right = kind == ASTTableJoin::Kind::Right;
    static constexpr bool inner = kind == ASTTableJoin::Kind::Inner;
    static constexpr bool full = kind == ASTTableJoin::Kind::Full;

    static constexpr bool left_semi_or_anti_with_inequal = left && (is_semi_join || is_anti_join);

    static constexpr bool right_semi_or_anti_with_inequal = right && (is_semi_join || is_anti_join);

    static constexpr bool left_all = is_all_join && left;
    static constexpr bool right_all = is_all_join && right;

    /// If we need to replicate any rows from the left table.
    static constexpr bool need_replication = is_all_join || inner || (right && (is_any_join || is_semi_join || is_anti_join)) || (left && (is_anti_join || is_semi_join));

    /// If we need to filter any rows from the left table.
    static constexpr bool need_filter = is_all_join || inner || (right && (is_any_join || is_semi_join || is_anti_join)) || (left && (is_anti_join || is_semi_join));

    static constexpr bool add_missing = (left || full) && !is_semi_join;

    static constexpr bool need_flags = MapGetter<kind, strictness>::flagged;
};

/// Join features without inequal condition
template <ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness>
struct JoinFeatures<kind, strictness, false>
{
    static constexpr bool is_any_join = strictness == ASTTableJoin::Strictness::Any;
    static constexpr bool is_all_join = strictness == ASTTableJoin::Strictness::All;
    static constexpr bool is_asof_join = strictness == ASTTableJoin::Strictness::Asof;
    static constexpr bool is_semi_join = strictness == ASTTableJoin::Strictness::Semi;
    static constexpr bool is_anti_join = strictness == ASTTableJoin::Strictness::Anti;

    static constexpr bool left = kind == ASTTableJoin::Kind::Left;
    static constexpr bool right = kind == ASTTableJoin::Kind::Right;
    static constexpr bool inner = kind == ASTTableJoin::Kind::Inner;
    static constexpr bool full = kind == ASTTableJoin::Kind::Full;

    static constexpr bool left_semi_or_anti_with_inequal = false;
    static constexpr bool right_semi_or_anti_with_inequal = false;

    static constexpr bool left_all = is_all_join && left;
    static constexpr bool right_all = is_all_join && right;

    /// If we need to replicate any rows from the left table.
    static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);

    /// If we need to filter any rows from the left table.
    static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));

    static constexpr bool add_missing = (left || full) && !is_semi_join;

    static constexpr bool need_flags = MapGetter<kind, strictness>::flagged;
};

HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , key_names_right(table_join->keyNamesRight())
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , inequal_condition_actions(table_join->getInequalCondition())
    , ineuqal_column_name(table_join->getInequalColumnName())
    , data(std::make_shared<RightTableData>())
    , right_sample_block(right_sample_block_)
    , log(getLogger("HashJoin"))
{
    LOG_DEBUG(log, "Right sample block: {}", right_sample_block.dumpStructure());

    if (inequal_condition_actions)
        validateInequalConditions(inequal_condition_actions);

    table_join->splitAdditionalColumns(right_sample_block, right_table_keys, sample_block_with_columns_to_add);
    required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);

    JoinCommon::removeLowCardinalityInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);

    ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right, table_join->keyIdsNullSafe());

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(sample_block_with_columns_to_add);

    if (table_join->dictionary_reader)
    {
        LOG_DEBUG(log, "Performing join over dict");
        data->type = Type::DICT;
        std::get<MapsOne>(data->maps).create(Type::DICT);
        chooseMethod(key_columns, key_sizes); /// init key_sizes
    }
    else if (strictness == ASTTableJoin::Strictness::Asof)
    {
        /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
        /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
        if (!isLeft(kind) && !isInner(kind))
            throw Exception("Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported", ErrorCodes::NOT_IMPLEMENTED);

        if (key_columns.size() <= 1)
            throw Exception("ASOF join needs at least one equi-join column", ErrorCodes::SYNTAX_ERROR);

        /// extractKeysForJoin() and materializeColumns() assume ASOF column should not be nullable,
        /// both functions need to be changed if the assumption has been changed in future
        if (right_table_keys.getByName(key_names_right.back()).type->isNullable())
            throw Exception("ASOF join over right table Nullable column is not implemented", ErrorCodes::NOT_IMPLEMENTED);

        size_t asof_size;
        asof_type = AsofRowRefs::getTypeSize(*key_columns.back(), asof_size);
        key_columns.pop_back();

        /// this is going to set up the appropriate hash table for the direct lookup part of the join
        /// However, this does not depend on the size of the asof join key (as that goes into the BST)
        /// Therefore, add it back in such that it can be extracted appropriately from the full stored
        /// key_columns and key_sizes
        init(chooseMethod(key_columns, key_sizes));
        key_sizes.push_back(asof_size);
    }
    else
    {
        /// Choose data structure to use for JOIN.
        init(chooseMethod(key_columns, key_sizes));
    }
}

HashJoin::Type HashJoin::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

    if (keys_size == 0)
        return Type::CROSS;

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (isColumnConst(*key_columns[0]) && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

template<typename KeyGetter, bool is_asof_join>
static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

class KeyGetterForDict
{
public:
    using Mapped = RowRef;
    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

    KeyGetterForDict(const TableJoin & table_join, const ColumnRawPtrs & key_columns)
    {
        table_join.dictionary_reader->readKeys(*key_columns[0], read_result, found, positions);

        for (ColumnWithTypeAndName & column : read_result)
            if (table_join.rightBecomeNullable(column.type))
                JoinCommon::convertColumnToNullable(column);
    }

    FindResult findKey(void *, size_t row, const Arena &)
    {
        result.block = &read_result;
        result.row_num = positions[row];
        return FindResult(&result, found[row], 0);
    }

private:
    Block read_result;
    Mapped result;
    ColumnVector<UInt8>::Container found;
    std::vector<size_t> positions;
};

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

constexpr bool use_offset = true;

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};


void HashJoin::init(Type type_)
{
    data->type = type_;

    if (kind == ASTTableJoin::Kind::Cross)
        return;
    joinDispatchInit(kind, strictness, data->maps, has_inequal_condition);
    joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { map.create(data->type); }, has_inequal_condition);
}

bool HashJoin::overDictionary() const
{
    return data->type == Type::DICT;
}

bool HashJoin::empty() const
{
    return data->type == Type::EMPTY;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    return isInnerOrRight(getKind()) && data->empty && !overDictionary();
}

size_t HashJoin::getTotalRowCount() const
{
    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & block : data->blocks)
            res += block.rows();
    }
    else if (data->type != Type::DICT)
    {
        joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { res += map.getTotalRowCount(data->type); }, has_inequal_condition);
    }

    return res;
}

size_t HashJoin::getTotalByteCount() const
{
    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & block : data->blocks)
            res += block.bytes();
    }
    else if (data->type != Type::DICT)
    {
        joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { res += map.getTotalByteCountImpl(data->type); }, has_inequal_condition);
        res += data->pool.size();
    }

    return res;
}

namespace
{
    /// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
    template <typename Map, typename KeyGetter>
    struct Inserter
    {
        static ALWAYS_INLINE void insertOne(const HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i,
                                            Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }

        static ALWAYS_INLINE void insertAll(const HashJoin &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
            else
            {
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.getMapped().insert({stored_block, i}, pool);
            }
        }

        static ALWAYS_INLINE void insertAsof(HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool,
                                             const IColumn & asof_column)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);
            typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

            TypeIndex asof_type = *join.getAsofType();
            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);
            time_series_map->insert(asof_type, asof_column, stored_block, i);
        }
    };


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    size_t NO_INLINE insertFromBlockImplTypeCase(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRef>;
        constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = key_columns.back();

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, *asof_column);
            else if constexpr (mapped_one)
                Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
        return map.getBufferSizeInCells();
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    size_t insertFromBlockImplType(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        if (null_map)
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        else
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t insertFromBlockImpl(
        HashJoin & join, HashJoin::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        switch (type)
        {
            case HashJoin::Type::EMPTY: return 0;
            case HashJoin::Type::CROSS: return 0; /// Do nothing. We have already saved block, and it is enough.
            case HashJoin::Type::DICT:  return 0; /// No one should call it with Type::DICT.

        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
        __builtin_unreachable();
    }
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->forceHashJoin() || isRightOrFull(kind) || table_join->getRuntimeFilterConsumer() != nullptr;
    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == ASTTableJoin::Strictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
        saved_block_sample.insert(column);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(saved_block_sample, (isFull(kind) ? right_table_keys.columns() : 0));
}

Block HashJoin::structureRightBlock(const Block & block) const
{
    Block structured_block;
    for (const auto & sample_column : savedBlockSample().getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);
        structured_block.insert(column);
    }

    return structured_block;
}

bool HashJoin::addJoinedBlock(const Block & source_block, bool check_limits)
{
    if (empty())
        throw Exception("Logical error: HashJoin was not initialized", ErrorCodes::LOGICAL_ERROR);
    if (overDictionary())
        throw Exception("Logical error: insert into hash-map in HashJoin over dictionary", ErrorCodes::LOGICAL_ERROR);

    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception("Too many rows in right table block for HashJoin: " + toString(source_block.rows()), ErrorCodes::NOT_IMPLEMENTED);

    /// There's no optimization for right side const columns. Remove constness if any.
    const auto *null_safe_columns = table_join->keyIdsNullSafe();
    Block block = materializeBlock(source_block);
    size_t rows = block.rows();

    ColumnRawPtrs key_columns = JoinCommon::materializeColumnsInplace(block, key_names_right, null_safe_columns);

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map, null_safe_columns);

    /// If RIGHT or FULL save blocks with nulls for NonJoinedBlockInputStream
    UInt8 save_nullmap = 0;
    if (isRightOrFull(kind) && null_map)
    {
        for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
            save_nullmap |= (*null_map)[i];
    }

    Block structured_block = structureRightBlock(block);
    size_t total_rows = 0;
    size_t total_bytes = 0;

    {
        if (storage_join_lock.mutex())
            throw DB::Exception("addJoinedBlock called when HashJoin locked to prevent updates",
                                ErrorCodes::LOGICAL_ERROR);

        data->blocks.emplace_back(std::move(structured_block));
        Block * stored_block = &data->blocks.back();
        if (inequal_condition_actions)
            data->used_map.emplace(reinterpret_cast<UInt64>(stored_block), VectorWithAlloc<bool>(rows));

        if (rows)
            data->empty = false;

        if (kind != ASTTableJoin::Kind::Cross)
        {
            joinDispatch(kind, strictness, data->maps, [&](auto  kind_, auto strictness_, auto & map)
            {
                size_t size = insertFromBlockImpl<strictness_>(*this, data->type, map, rows, key_columns, key_sizes, stored_block, null_map, data->pool);
                /// Number of buckets + 1 value from zero storage
                used_flags.reinit<kind_, strictness_>(size + 1);
            }, has_inequal_condition);
        }

        if (save_nullmap)
            data->blocks_nullmaps.emplace_back(stored_block, null_map_holder);

        if (!check_limits)
            return true;

        /// TODO: Do not calculate them every time
        total_rows = getTotalRowCount();
        total_bytes = getTotalByteCount();
    }

    // LOG_ERROR(log, "add join block:{} {}", total_rows, total_bytes);

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


namespace
{

template <bool lazy>
class AddedColumns
{
public:
    struct TypeAndName
    {
        DataTypePtr type;
        String name;
        String qualified_name;

        TypeAndName(DataTypePtr type_, const String & name_, const String & qualified_name_)
            : type(type_), name(name_), qualified_name(qualified_name_)
        {
        }
    };

    struct LazyOutput
    {
        PaddedPODArray<UInt64> rows_with_block;
    };

    AddedColumns(
        const Block & block_with_columns_to_add,
        const Block & block,
        const Block & saved_block_sample,
        const HashJoin & join,
        const ColumnRawPtrs & key_columns_,
        const Sizes & key_sizes_,
        bool is_asof_join,
        bool is_join_get_)
        : key_columns(key_columns_)
        , key_sizes(key_sizes_)
        , left_block(block)
        , rows_to_add(block.rows())
        , asof_type(join.getAsofType())
        , asof_inequality(join.getAsofInequality())
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        if constexpr (lazy)
        {
            has_columns_to_add = num_columns_to_add > 0;
            lazy_output.rows_with_block.reserve(rows_to_add);
        }

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!block.has(qualified_name))
                addColumn(src_column, qualified_name);
        }

        if (is_asof_join)
        {
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column, right_asof_column.name);
            left_asof_key = key_columns.back();
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));

        nullable_column_ptrs.resize(right_indexes.size(), nullptr);
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
            const auto & saved_column = saved_block_sample.getByPosition(right_indexes[j]).column;
            if (columns[j]->isNullable() && !saved_column->isNullable())
                nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
        }
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }
    void buildOutput();

    void appendFromBlock(const RowRef * ref, bool has_defaults);
    void appendFromBlockWithIndex(const RowRef * ref, bool has_defaults);

    void appendDefaultRow();

    void applyLazyDefaults();

    TypeIndex asofType() const { return *asof_type; }
    ASOF::Inequality asofInequality() const { return asof_inequality; }
    const IColumn & leftAsofKey() const { return *left_asof_key; }
    const ColumnRawPtrs & key_columns;
    const Sizes & key_sizes;
    const Block & left_block;
    size_t max_joined_block_rows = 0; // no more than 2x DEFAULT_BLOCK_SIZE
    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;
    IColumn::Filter filter;

    void reserve(bool /*need_replicate*/)
    {
        // if (!max_joined_block_rows)
        //     return;

        // /// Do not allow big allocations when user set max_joined_block_rows to huge value
        // size_t reserve_size = std::min<size_t>(max_joined_block_rows, kMaxAllowedJoinedBlockRows);

        // if (need_replicate)
        //     /// Reserve 10% more space for columns, because some rows can be repeated
        //     reserve_size = static_cast<size_t>(1.1 * reserve_size);

        // for (auto & column : columns)
        //     column->reserve(reserve_size);
    }

    VectorWithAlloc<std::pair<UInt64, size_t>> right_anti_index;

private:
    std::vector<TypeAndName> type_name;
    MutableColumns columns;
    std::vector<size_t> right_indexes;
    std::vector<ColumnNullable *> nullable_column_ptrs;
    size_t lazy_defaults_count = 0;

    /// for lazy
    // The default row is represented by an empty RowRef, so that fixed-size blocks can be generated sequentially,
    // default_count cannot represent the position of the row
    LazyOutput lazy_output;
    bool has_columns_to_add;

    /// for ASOF
    std::optional<TypeIndex> asof_type;
    ASOF::Inequality asof_inequality;
    const IColumn * left_asof_key = nullptr;
    bool is_join_get;

    void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name, qualified_name);
    }

    void checkBlock(const Block & block)
    {
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            const auto * column_from_block = block.getByPosition(right_indexes[j]).column.get();
            const auto * dest_column = columns[j].get();
            if (auto * nullable_col = nullable_column_ptrs[j])
            {
                if (!is_join_get)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Columns {} and {} can have different nullability only in joinGetOrNull",
                                    dest_column->getName(), column_from_block->getName());
                dest_column = nullable_col->getNestedColumnPtr().get();
            }
            /** Using dest_column->structureEquals(*column_from_block) will not work for low cardinality columns,
              * because dictionaries can be different, while calling insertFrom on them is safe, for example:
              * ColumnLowCardinality(size = 0, UInt8(size = 0), ColumnUnique(size = 1, String(size = 1)))
              * and
              * ColumnLowCardinality(size = 0, UInt16(size = 0), ColumnUnique(size = 1, String(size = 1)))
              */
            if (typeid(*dest_column) != typeid(*column_from_block))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} have different types {} and {}",
                                dest_column->getName(), column_from_block->getName(),
                                demangle(typeid(*dest_column).name()), demangle(typeid(*column_from_block).name()));
        }
    }
};

template<> void AddedColumns<false>::buildOutput()
{
}

template<>
void AddedColumns<true>::buildOutput()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto& col = columns[i];
        col->reserve(col->size() + lazy_output.rows_with_block.size());
        size_t default_count = 0;
        auto apply_default = [&]()
        {
            if (default_count > 0)
            {
                JoinCommon::addDefaultValues(*col, type_name[i].type, default_count);
                default_count = 0;
            }
        };

        if (is_join_get)
        {
            for (UInt64 j : lazy_output.rows_with_block)
            {
                if (!j)
                {
                    default_count++;
                    continue;
                }
                apply_default();
                const RowRef * ref = reinterpret_cast<RowRef *>(j);
                const auto & column_from_block = ref->block->getByPosition(right_indexes[i]);
                /// If it's joinGetOrNull, we need to wrap not-nullable columns in StorageJoin.
                if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get());
                    nullable_col && !column_from_block.column->isNullable())
                {
                    nullable_col->insertFromNotNullable(*column_from_block.column, ref->row_num);
                }
            }
        }
        else
        {
            for (UInt64 j : lazy_output.rows_with_block)
            {
                if (!j)
                {
                    default_count++;
                    continue;
                }
                apply_default();
                const RowRef * ref = reinterpret_cast<RowRef *>(j);
                const auto & column_from_block = ref->block->getByPosition(right_indexes[i]);
                col->insertFrom(*column_from_block.column, ref->row_num);
            }
        }

        apply_default();
    }
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
        lazy_defaults_count = 0;
    }
}

template<>
void AddedColumns<true>::applyLazyDefaults()
{
}

template <>
void AddedColumns<false>::appendFromBlock(const RowRef *ref,const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

    if (is_join_get)
    {
        /// If it's joinGetOrNull, we need to wrap not-nullable columns in StorageJoin.
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
        {
            const auto & column = ref->block->getByPosition(right_indexes[j]).column;
            if (auto * nullable_col = nullable_column_ptrs[j])
                nullable_col->insertFromNotNullable(*column, ref->row_num);
            else
                columns[j]->insertFrom(*column, ref->row_num);
        }
    }
    else
    {
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
        {
            columns[j]->insertFrom(*ref->block->getByPosition(right_indexes[j]).column, ref->row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const RowRef * ref, bool)
{
#ifndef NDEBUG
    checkBlock(*ref->block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.rows_with_block.emplace_back(reinterpret_cast<UInt64>(ref));
    }
}
template<>
void AddedColumns<false>::appendDefaultRow()
{
    ++lazy_defaults_count;
}

template<>
void AddedColumns<true>::appendDefaultRow()
{
    if (has_columns_to_add)
    {
        lazy_output.rows_with_block.emplace_back(0);
    }
}

template<>
void AddedColumns<false>::appendFromBlockWithIndex(const RowRef * ref, bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

    for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
    {
        columns[j]->insertFrom(*(ref->block->getByPosition(right_indexes[j]).column), ref->row_num);
    }

    right_anti_index.emplace_back(reinterpret_cast<UInt64>(ref->block), ref->row_num);
}

template<>
void AddedColumns<true>::appendFromBlockWithIndex(const RowRef * ref, bool)
{
#ifndef NDEBUG
    checkBlock(*ref->block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.rows_with_block.emplace_back(reinterpret_cast<UInt64>(ref));
    }
    right_anti_index.emplace_back(reinterpret_cast<UInt64>(ref->block), ref->row_num);
}

template <typename Map, bool add_missing, typename AddedColumns>
void addFoundRowAll(const typename Map::mapped_type & mapped, AddedColumns & added, IColumn::Offset & current_offset [[maybe_unused]], bool use_row_flag = false)
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    /// During template initiation prevent Map::mapped_type is RowRef
    /// In inequal condition left semi and anti need RowRefList as right data type
    /// TODO: Find better solution on template initiation
    if constexpr (std::is_same_v<std::remove_reference_t<typename Map::mapped_type>, RowRefList>)
    {
        if (use_row_flag)
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                added.appendFromBlockWithIndex(it.ptr(), false);
                ++current_offset;
            }
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                added.appendFromBlock(it.ptr(), false);
                ++current_offset;
            }
        }
    }
};

template <bool add_missing, bool need_offset, typename AddedColumns>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]])
{
    if constexpr (need_filter)
        filter[pos] = 1;
}


/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map, bool has_inequal_condition, typename AddedColumns>
NO_INLINE size_t joinRightColumns(
    KeyGetter && key_getter,
    const Map & map,
    AddedColumns & added_columns,
    const ConstNullMapPtr & null_map [[maybe_unused]],
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    JoinFeatures<KIND, STRICTNESS, has_inequal_condition> join_feature;

    size_t rows = added_columns.rows_to_add;
    if constexpr (need_filter)
        added_columns.filter = IColumn::Filter(rows, 0);

    Arena pool;

    if constexpr (join_feature.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;
    size_t max_joined_block_rows = added_columns.max_joined_block_rows;

    size_t i = 0;
    for (; i < rows; ++i)
    {
        if constexpr (join_feature.need_replication)
        {
            if (unlikely(current_offset > max_joined_block_rows))
            {
                added_columns.offsets_to_replicate->resize(i);
                added_columns.filter.resize(i);
                break;
            }

        }

        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                addNotFoundRow<join_feature.add_missing, join_feature.need_replication>(added_columns, current_offset);

                if constexpr (join_feature.need_replication)
                    (*added_columns.offsets_to_replicate)[i] = current_offset;
                continue;
            }
        }

        auto find_result = key_getter.findKey(map, i, pool);

        if (find_result.isFound())
        {
            auto & mapped = find_result.getMapped();

            if constexpr (join_feature.is_asof_join)
            {
                TypeIndex asof_type = added_columns.asofType();
                ASOF::Inequality asof_inequality = added_columns.asofInequality();
                const IColumn & left_asof_key = added_columns.leftAsofKey();

                if (const RowRef * found = mapped.findAsof(asof_type, asof_inequality, left_asof_key, i))
                {
                    setUsed<need_filter>(added_columns.filter, i);
                    used_flags.template setUsed<join_feature.need_flags>(find_result.getOffset());
                    added_columns.appendFromBlock(found, join_feature.add_missing);
                }
                else
                    addNotFoundRow<join_feature.add_missing, join_feature.need_replication>(added_columns, current_offset);
            }
            else if constexpr (join_feature.is_all_join || join_feature.left_semi_or_anti_with_inequal)
            {
                /// When left anti with inequal condition not set filter flag
                if constexpr ((has_inequal_condition && join_feature.left && join_feature.is_semi_join) || join_feature.is_all_join)
                    setUsed<need_filter>(added_columns.filter, i);

                /// WHen right outer join with inequal condition not set used_flags use right_anti_index to add nonjoined rows
                if constexpr (!(has_inequal_condition && join_feature.right && join_feature.is_all_join))
                   used_flags.template setUsed<join_feature.need_flags>(find_result.getOffset());

                /// For join dictionary data map type is nullptr this condition prevents template initiation failure
                /// TODO: Find better solution on template initiation
                if constexpr (!std::is_null_pointer_v<std::remove_cvref_t<Map>>)
                {
                    if constexpr (has_inequal_condition && join_feature.right && join_feature.is_all_join)
                       addFoundRowAll<Map, join_feature.add_missing>(mapped, added_columns, current_offset, true);
                    else
                       addFoundRowAll<Map, join_feature.add_missing>(mapped, added_columns, current_offset);
                }
            }
            else if constexpr ((join_feature.is_any_join || join_feature.is_semi_join) && join_feature.right)
            {
                /// Use first appeared left key + it needs left columns replication
                bool used_once = used_flags.template setUsedOnce<join_feature.need_flags>(find_result.getOffset());

                if (used_once || has_inequal_condition)
                {
                    setUsed<need_filter>(added_columns.filter, i);
                    if constexpr (has_inequal_condition)
                        addFoundRowAll<Map, join_feature.add_missing>(mapped, added_columns, current_offset, true);
                    else
                        addFoundRowAll<Map, join_feature.add_missing>(mapped, added_columns, current_offset);
                }
            }
            else if constexpr (join_feature.is_any_join && join_feature.inner)
            {
                bool used_once = used_flags.template setUsedOnce<join_feature.need_flags>(find_result.getOffset());

                /// Use first appeared left key only
                if (used_once)
                {
                    setUsed<need_filter>(added_columns.filter, i);
                    added_columns.appendFromBlock(&mapped, join_feature.add_missing);
                }
            }
            else if constexpr (join_feature.is_any_join && join_feature.full)
            {
                /// TODO
            }
            else if constexpr (join_feature.is_anti_join)
            {
                if constexpr (join_feature.right && join_feature.need_flags)
                {
                    if constexpr (has_inequal_condition)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        addFoundRowAll<Map, join_feature.add_missing>(mapped, added_columns, current_offset, true);
                    }
                    else
                        used_flags.template setUsed<join_feature.need_flags>(find_result.getOffset());
                }
            }
            else /// ANY LEFT, SEMI LEFT, RightAny
            {
                setUsed<need_filter>(added_columns.filter, i);
                used_flags.template setUsed<join_feature.need_flags>(find_result.getOffset());
                added_columns.appendFromBlock(&mapped, join_feature.add_missing);
            }
        }
        else
        {
            if constexpr (join_feature.is_anti_join && join_feature.left)
                setUsed<join_feature.need_filter>(added_columns.filter, i);
            addNotFoundRow<join_feature.add_missing, join_feature.need_replication>(added_columns, current_offset);
        }

        if constexpr (join_feature.need_replication)
            (*added_columns.offsets_to_replicate)[i] = current_offset;
    }

    added_columns.applyLazyDefaults();
    return i;
}

constexpr std::array<bool, 2> NEED_FILTERS = {
    false,
    true
};

constexpr std::array<bool, 2> HAS_NULL_MAPS = {
    false,
    true
};

constexpr std::array<bool, 2> HAS_INEUQALS = {
    false,
    true
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, typename AddedColumns>
size_t joinRightColumnsSwitchNullability(
        KeyGetter && key_getter,
        const Map & map,
        AddedColumns & added_columns,
        const ConstNullMapPtr & null_map [[maybe_unused]],
        JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
        bool has_inequal_condition [[maybe_unused]])
{
    bool need_filter = added_columns.need_filter;
    bool has_null_map = null_map != nullptr;
    size_t num_joined;

    /// Same as joinDispatch use need row filter, has null map and has inequal condition for different actions
    static_for<0, NEED_FILTERS.size() * HAS_NULL_MAPS.size() * HAS_INEUQALS.size()>([&](auto ijk)
    {
        constexpr auto k = ijk / (NEED_FILTERS.size() * HAS_NULL_MAPS.size());
        constexpr auto j = (ijk / NEED_FILTERS.size()) % HAS_NULL_MAPS.size();
        constexpr auto i = ijk % NEED_FILTERS.size();
        if (need_filter == NEED_FILTERS[i] && has_null_map == HAS_NULL_MAPS[j] && has_inequal_condition == HAS_INEUQALS[k])
        {
            num_joined = joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, NEED_FILTERS[i], HAS_NULL_MAPS[j], HAS_INEUQALS[k]>(
                    std::forward<KeyGetter>(key_getter), map, added_columns, null_map, used_flags);
        }
    });
    return num_joined;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps, typename AddedColumns>
size_t switchJoinRightColumns(
    const Maps & maps_,
    AddedColumns & added_columns,
    HashJoin::Type type,
    const ConstNullMapPtr & null_map,
    JoinStuff::JoinUsedFlags & used_flags,
    bool has_inequal_condition)
{
    constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
    switch (type)
    {
    #define M(TYPE) \
        case HashJoin::Type::TYPE: \
        { \
            using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, const std::remove_reference_t<decltype(*maps_.TYPE)>>::Type; \
            auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(added_columns.key_columns, added_columns.key_sizes); \
            return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetter>( \
                std::move(key_getter), *maps_.TYPE, added_columns, null_map, used_flags, has_inequal_condition); \
        }
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(type)), ErrorCodes::UNSUPPORTED_JOIN_KEYS);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename AddedColumns>
size_t dictionaryJoinRightColumns(const TableJoin & table_join, AddedColumns & added_columns, const ConstNullMapPtr & null_map)
{
    if constexpr (KIND == ASTTableJoin::Kind::Left &&
        (STRICTNESS == ASTTableJoin::Strictness::Any ||
        STRICTNESS == ASTTableJoin::Strictness::Semi ||
        STRICTNESS == ASTTableJoin::Strictness::Anti))
    {
        assert(added_columns.key_columns.size() == 1);

        JoinStuff::JoinUsedFlags flags;
        KeyGetterForDict key_getter(table_join, added_columns.key_columns);
        return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetterForDict>(
                std::move(key_getter), nullptr, added_columns, null_map, flags, false);
    }

    throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
}

/// Cut first num_rows rows from block in place and returns block with remaining rows
Block sliceBlock(Block & block, size_t num_rows)
{
    size_t total_rows = block.rows();
    if (num_rows >= total_rows)
        return {};
    size_t remaining_rows = total_rows - num_rows;
    Block remaining_block = block.cloneEmpty();
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & col = block.getByPosition(i);
        remaining_block.getByPosition(i).column = col.column->cut(num_rows, remaining_rows);
        col.column = col.column->cut(0, num_rows);
    }
    return remaining_block;
}

} /// nameless


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool is_dict, typename Maps>
Block HashJoin::joinBlockImpl(
    Block & block,
    const Names & key_names_left,
    const Block & block_with_columns_to_add,
    const Maps & maps_,
    bool is_join_get) const
{
    JoinFeatures<KIND, STRICTNESS, false> join_feature;

    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    const auto *null_safe_columns = table_join->keyIdsNullSafe();
    Columns materialized_keys = JoinCommon::materializeColumns(block, key_names_left, null_safe_columns);
    ColumnRawPtrs left_key_columns = JoinCommon::getRawPointers(materialized_keys);

    /// Keys with NULL value in any column won't join to anything.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(left_key_columns, null_map, null_safe_columns);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if constexpr (join_feature.right || join_feature.full)
    {
        materializeBlockInplace(block);
        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns<!join_feature.is_any_join && !is_dict> added_columns(
        block_with_columns_to_add, block, savedBlockSample(), *this, left_key_columns, key_sizes, join_feature.is_asof_join, is_join_get);
    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = join_feature.need_filter || has_required_right_keys;
    Stopwatch watch;
    added_columns.max_joined_block_rows = table_join->maxJoinedBlockRows();
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_feature.need_replication);

    size_t num_joined = is_dict ?
        dictionaryJoinRightColumns<KIND, STRICTNESS>(*table_join, added_columns, null_map) :
        switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, data->type, null_map, used_flags, false);
    Block remaining_block = sliceBlock(block, num_joined);
    added_columns.buildOutput();
    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if (join_feature.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(added_columns.filter, -1);

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            const auto & left_name = required_right_keys_sources[i];

            /// asof column is already in block.
            if (join_feature.is_asof_join && right_key.name == key_names_right.back())
                continue;

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();

            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            ColumnWithTypeAndName right_col(col.column, col.type, right_col_name);
            if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                JoinCommon::changeLowCardinalityInplace(right_col);
            right_col = correctNullability(std::move(right_col), is_nullable);
            block.insert(right_col);
        }
    }
    else if (has_required_right_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(added_columns.filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            const auto & left_name = required_right_keys_sources[i];

            /// asof column is already in block.
            if (join_feature.is_asof_join && right_key.name == key_names_right.back())
                continue;

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();

            ColumnPtr thin_column = filterWithBlanks(col.column, filter);

            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            ColumnWithTypeAndName right_col(thin_column, col.type, right_col_name);
            if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                JoinCommon::changeLowCardinalityInplace(right_col);
            right_col = correctNullability(std::move(right_col), is_nullable, null_map_filter);
            block.insert(right_col);

            if (join_feature.need_replication)
                right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
        }
    }

    if (join_feature.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }

    return remaining_block;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool is_dict, typename Maps>
Block HashJoin::joinBlockImplIneuqalCondition(
    Block & block,
    const Names & key_names_left,
    const Block & block_with_columns_to_add,
    const Maps & maps_,
    bool is_join_get) const
{
    JoinFeatures<KIND, STRICTNESS, true> join_feature;

    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    const auto *null_safe_columns = table_join->keyIdsNullSafe();
    Columns materialized_keys = JoinCommon::materializeColumns(block, key_names_left, null_safe_columns);
    ColumnRawPtrs left_key_columns = JoinCommon::getRawPointers(materialized_keys);

    /// Keys with NULL value in any column won't join to anything.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(left_key_columns, null_map, null_safe_columns);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if constexpr (join_feature.right || join_feature.full)
    {
        materializeBlockInplace(block);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns<!join_feature.is_any_join && !is_dict> added_columns(
        block_with_columns_to_add, block, savedBlockSample(), *this, left_key_columns, key_sizes, join_feature.is_asof_join, is_join_get);
    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = join_feature.need_filter || has_required_right_keys;

    Stopwatch watch;
    added_columns.max_joined_block_rows = table_join->maxJoinedBlockRows();
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_feature.need_replication);

    size_t num_joined = is_dict ?
        dictionaryJoinRightColumns<KIND, STRICTNESS>(*table_join, added_columns, null_map) :
        switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, data->type, null_map, used_flags, true);

    Block remaining_block = sliceBlock(block, num_joined);

    NameSet left_block_name_set = block.getNameSet();
    added_columns.buildOutput();
    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];
    if (has_required_right_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(added_columns.filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            const auto & left_name = required_right_keys_sources[i];

            /// asof column is already in block.
            if (join_feature.is_asof_join && right_key.name == key_names_right.back())
                continue;

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();

            ColumnPtr thin_column = filterWithBlanks(col.column, filter);

            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            ColumnWithTypeAndName right_col(thin_column, col.type, right_col_name);
            if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                JoinCommon::changeLowCardinalityInplace(right_col);
            right_col = correctNullability(std::move(right_col), is_nullable, null_map_filter);
            block.insert(right_col);

            if (join_feature.need_replication)
                right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
        }
        added_columns.filter.swap(null_map_filter.getData());
    }

    if constexpr (join_feature.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }

    /**
    * Logic for inequal join
    * 1 for equal conditions, it is joined by hashtable
    * 2 for inequal conditions, it is filtered by expression actions
    *   - for semi right, we need remove duplicated according to offsets_to_replicate
    *   - for anti right, we introduce right_anti_index and used_map
    *      -- right_anti_index is used in `hashtable find logical`, it records which row is select in anti join.
    *      -- used_map is used in right added column, it records the rows which will be select in the final output.
    */
    if (!block.rows())
        return remaining_block;

    Block matched_block = block;
    inequal_condition_actions->execute(matched_block);
    ColumnPtr filter_column = matched_block.getByName(ineuqal_column_name).column;
    if (const auto * nullable_filter = checkAndGetColumn<ColumnNullable>(*filter_column))
        filter_column = nullable_filter->getNestedColumnWithDefaultOnNull();

    const IColumn::Filter & filter_column_data = typeid_cast<const ColumnVector<UInt8> &>(*filter_column).getData();

    /// semi join
    if constexpr (join_feature.is_semi_join)
    {
        if constexpr (join_feature.right) /// right semi
        {
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            auto & right_anti_index = added_columns.right_anti_index;
            auto & used_map = data->used_map;
            /**
            ** hold a lock for parallel joinImpl, avoiding modifying used_map in parallel, otherwise we cannot get correct mutable_filter;
            ** if the data is orthogonal, the lock is useless and therre is no performance degradation since there is no multiple threads visiting
            ** the same HashJoin object (they are independent).
            ** if the data is not orthogonal, the performance degradation is minimal according to the performance test on TPC-DS / TPC-H.
            */
            auto lock = std::lock_guard<std::mutex>(data->mutex);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (added_columns.filter[i])
                {
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (mutable_filter[index])
                        {
                            auto & right_index = right_anti_index[index];
                            auto & right_index_used_flags = used_map[right_index.first];
                            if (right_index_used_flags[right_index.second])
                                mutable_filter[index] = 0;
                            else
                                right_index_used_flags[right_index.second] = true;
                        }
                    }
                }
            }
        }
        else if constexpr (join_feature.left) /// left semi
        {
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (added_columns.filter[i]) /// consider only matched rows
                {
                    bool any_match_rows = false;
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (any_match_rows) /// remove other depulicate rows, keep first matched one
                            mutable_filter[index] = 0;
                        else
                        {
                            if (mutable_filter[index]) /// inequal condition is true , record matched rows as first row
                                any_match_rows = true;
                        }
                    }
                }
            }
        }
    }

    /// anti join
    if constexpr(join_feature.is_anti_join)
    {
        if constexpr (join_feature.right) /// right anti
        {
            auto & right_anti_index = added_columns.right_anti_index;
            auto & used_map = data->used_map;
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            auto lock = std::lock_guard<std::mutex>(data->mutex);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (added_columns.filter[i])
                {
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (mutable_filter[index])
                        {
                            auto & right_index = right_anti_index[index];
                            used_map[right_index.first][right_index.second] = true;
                            mutable_filter[index] = 0;
                        }
                    }
                }
            }
        }
        else if constexpr (join_feature.left) /// left anti
        {
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (!added_columns.filter[i]) /// meet equal condition
                {
                    bool any_match_rows = false;
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (any_match_rows)
                            mutable_filter[index] = 0;
                        else
                        {
                            if (mutable_filter[index]) /// if one row meet inequal condition, all rows are removed
                            {
                                mutable_filter[index] = 0;
                                any_match_rows = true;
                            }
                        }
                    }
                    if (!any_match_rows) /// if there is no row meet inequal condition ,keep left first row
                        mutable_filter[offsets_to_replicate[i-1]] = 1;
                }
                else /// not match rows with row filter = true, keep these rows
                {
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        mutable_filter[index] = 1;
                    }
                }
            }
        }
    }

    /// outer join
    IColumn::Filter offsets_set_to_default(block.rows(), 0);
    if constexpr (join_feature.is_all_join)
    {
        if constexpr (join_feature.right) /// right outer
        {
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            auto & right_anti_index = added_columns.right_anti_index;
            auto & used_map = data->used_map;
            auto lock = std::lock_guard<std::mutex>(data->mutex);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (added_columns.filter[i])
                {
                    bool any_match_rows = false;
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (mutable_filter[index]) /// when meet condition set as used rows
                        {
                            auto & right_index = right_anti_index[index];
                            used_map[right_index.first][right_index.second] = true;
                            any_match_rows = true;
                        }
                    }
                    if (!any_match_rows) /// when not meet condition filter this row later add in NonJoinedBlockInputStream
                        mutable_filter[offsets_to_replicate[i-1]] = 0;
                }
                else
                {
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        mutable_filter[index] = 0;
                    }
                }
            }
        }
        else if constexpr (join_feature.left) /// left outer
        {
            const IColumn::Offsets & offsets_to_replicate = *added_columns.offsets_to_replicate;
            auto & mutable_filter = const_cast<IColumn::Filter &>(filter_column_data);
            for (size_t i = 0; i < added_columns.filter.size(); ++i)
            {
                if (added_columns.filter[i])
                {
                    bool any_match_rows = false;
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        if (mutable_filter[index])
                            any_match_rows = true;
                    }
                    if (!any_match_rows)
                    {
                        mutable_filter[offsets_to_replicate[i-1]] = 1;
                        offsets_set_to_default[offsets_to_replicate[i-1]] = 1;
                    }
                }
                else
                {
                    for (size_t j = 0; j < offsets_to_replicate[i] - offsets_to_replicate[i-1]; ++j)
                    {
                        size_t index = offsets_to_replicate[i-1] + j;
                        mutable_filter[index] = 1;
                    }
                }
            }
        }
    }

    for (size_t i = 0, size = block.columns(); i < size; ++i)
    {
        auto & column_type = block.safeGetByPosition(i);

        /// left outer join set not match column to null
        if constexpr (join_feature.left_all)
        {
            if (!left_block_name_set.contains(column_type.name))
            {
                if (auto * column_nullable = const_cast<ColumnNullable *>(checkAndGetColumn<ColumnNullable>(*(column_type.column))))
                    column_nullable->setNullAt(offsets_set_to_default);
                else
                {
                    MutableColumnPtr mutate_column = IColumn::mutate(std::move(column_type.column));
                    ColumnPtr replaced_col = mutate_column->replaceWithDefaultValue(offsets_set_to_default);
                    column_type.column = std::move(replaced_col);
                }
            }
        }
        column_type.column = column_type.column->filter(filter_column_data, -1);
    }

    return remaining_block;
}

void HashJoin::joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const
{
    size_t max_joined_block_rows = table_join->maxJoinedBlockRows();
    if (!max_joined_block_rows)
        max_joined_block_rows = kMaxAllowedJoinedBlockRows;

    size_t start_left_row = 0;
    size_t start_right_block = 0;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        not_processed.reset();
    }

    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (const ColumnWithTypeAndName & left_column : block)
        {
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : sample_block_with_columns_to_add)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        for (auto & dst : dst_columns)
            dst->reserve(max_joined_block_rows);
    }

    size_t rows_left = block.rows();
    size_t rows_added = 0;

    for (size_t left_row = start_left_row; left_row < rows_left; ++left_row)
    {
        size_t block_number = 0;
        for (const Block & block_right : data->blocks)
        {
            ++block_number;
            if (block_number < start_right_block)
                continue;

            size_t rows_right = block_right.rows();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *block_right.getByPosition(col_num).column;
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1});
            not_processed->block.swap(block);
            break;
        }
    }

    for (const ColumnWithTypeAndName & src_column : sample_block_with_columns_to_add)
        block.insert(src_column);

    block = block.cloneWithColumns(std::move(dst_columns));
}

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            "Number of arguments for function joinGet" + toString(or_null ? "OrNull" : "")
                + " doesn't match: passed, should be equal to " + toString(num_keys),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(
                "Type mismatch in joinGet key " + toString(i) + ": found type " + left_type->getName() + ", while the needed type is "
                    + right_type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception("StorageJoin doesn't contain column " + column_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null)
        elem.type = makeNullable(elem.type);
    return elem.type;
}

/// TODO: return multiple columns as named tuple
/// TODO: return array of values when strictness == ASTTableJoin::Strictness::All
ColumnWithTypeAndName HashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add) const
{
    bool is_valid = (strictness == ASTTableJoin::Strictness::Any || strictness == ASTTableJoin::Strictness::RightAny)
        && kind == ASTTableJoin::Kind::Left;
    if (!is_valid)
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);

    /// Assemble the key block with correct names.
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    static_assert(!MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>::flagged,
                  "joinGet are not protected from hash table changes between block processing");
    if (overDictionary())
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any, true>(
            keys, key_names_right, block_with_columns_to_add, std::get<MapsOne>(data->maps), /* is_join_get  */ true);
    else
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any, false>(
            keys, key_names_right, block_with_columns_to_add, std::get<MapsOne>(data->maps), /* is_join_get  */ true);

    return keys.getByPosition(keys.columns() - 1);
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    const Names & key_names_left = table_join->keyNamesLeft();
    JoinCommon::checkTypesOfKeys(block, key_names_left, right_table_keys, key_names_right);
    Block remaining_block;
    if (overDictionary())
    {
        using Kind = ASTTableJoin::Kind;
        using Strictness = ASTTableJoin::Strictness;

        auto & map = std::get<MapsOne>(data->maps);
        if (kind == Kind::Left)
        {
            switch (strictness)
            {
                case Strictness::Any:
                case Strictness::All:
                    remaining_block = joinBlockImpl<Kind::Left, Strictness::Any, true>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                case Strictness::Semi:
                    remaining_block = joinBlockImpl<Kind::Left, Strictness::Semi, true>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                case Strictness::Anti:
                    remaining_block = joinBlockImpl<Kind::Left, Strictness::Anti, true>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                default:
                    throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
            }
        }
        else if (kind == Kind::Inner && strictness == Strictness::All)
            remaining_block = joinBlockImpl<Kind::Left, Strictness::Semi, true>(block, key_names_left, sample_block_with_columns_to_add, map);
        else
            throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
    }
    else if (joinDispatch(kind, strictness, data->maps, [&](auto kind_, auto strictness_, auto & map)
        {
            if (has_inequal_condition) {
                remaining_block = joinBlockImplIneuqalCondition<kind_, strictness_, false>(block, key_names_left, sample_block_with_columns_to_add, map);
            } else {
                remaining_block = joinBlockImpl<kind_, strictness_, false>(block, key_names_left, sample_block_with_columns_to_add, map);
            }
        }, has_inequal_condition))
    {
        /// Joined
    }
    else if (kind == ASTTableJoin::Kind::Cross) {
        joinBlockImplCross(block, not_processed);
        return;
    }
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    if (remaining_block.rows())
        not_processed = std::make_shared<ExtraBlock>(ExtraBlock{std::move(remaining_block)});
    else
        not_processed.reset();
}

/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : private NotJoined, public IBlockInputStream
{
public:
    NonJoinedBlockInputStream(const HashJoin & parent_, const Block & result_sample_block_, UInt64 max_block_size_)
        : NotJoined(*parent_.table_join,
                    parent_.savedBlockSample(),
                    parent_.right_sample_block,
                    result_sample_block_)
        , parent(parent_)
        , max_block_size(max_block_size_)
    {}

    String getName() const override { return "NonJoined"; }
    Block getHeader() const override { return result_sample_block; }

protected:
    Block readImpl() override
    {
        if (parent.data->blocks.empty())
            return Block();
        return createBlock();
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;

    std::any position;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;

    Block createBlock()
    {
        MutableColumns columns_right = saved_block_sample.cloneEmptyColumns();

        size_t rows_added = 0;

        auto fill_callback = [&](auto, auto strictness, auto & map)
        {
            rows_added = fillColumnsFromMap<strictness>(map, columns_right);
        };

        if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps, fill_callback, parent.has_inequal_condition))
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);

        fillNullsFromBlocks(columns_right, rows_added);
        if (!rows_added)
            return {};

        correctLowcardAndNullability(columns_right);

        Block res = result_sample_block.cloneEmpty();
        addLeftColumns(res, rows_added);
        addRightColumns(res, columns_right);
        copySameKeys(res);
        return res;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parent.data->type)
        {
        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
            default:
                throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(parent.data->type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        __builtin_unreachable();
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        using Mapped = typename Map::mapped_type;
        using Iterator = typename Map::const_iterator;

        size_t rows_added = 0;

        if (!position.has_value())
            position = std::make_any<Iterator>(map.begin());

        Iterator & it = std::any_cast<Iterator &>(position);
        auto end = map.end();

        const static size_t BATCH_SIZE = 512;
        size_t batch_pos = 0;
        UInt64 rows_with_block[BATCH_SIZE];
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        const auto populate_col = [&]()
        {
            for (size_t j = 0; j < columns_keys_and_right.size(); ++j)
            {
                columns_keys_and_right[j]->reserve(columns_keys_and_right[j]->size() + batch_pos);
                for (size_t ind = 0; ind < batch_pos; ++ind)
                {
                    const RowRef * ref = reinterpret_cast<RowRef *>(rows_with_block[ind]);
                    const auto & mapped_column = ref->block->getByPosition(j).column;
                    columns_keys_and_right[j]->insertFrom(*mapped_column, ref->row_num);
                }
            }
            batch_pos = 0;
        };

        for (; it != end; ++it)
        {
            const Mapped & mapped = it->getMapped();

            size_t off = map.offsetInternal(it.getPtr());
            if (parent.isUsed(off))
                continue;

            const auto & right_data = parent.data;

            if constexpr (mapped_asof)
            {
                /// Do nothing
            }
            else if constexpr (mapped_one)
            {
                if (right_data->checkUsed(mapped.block, mapped.row_num))
                    continue;

                rows_with_block[batch_pos++] = reinterpret_cast<UInt64>(&mapped);
                ++rows_added;
            }
            else
            {
                for (auto itt = mapped.begin(); itt.ok(); ++itt)
                {
                    if (right_data->checkUsed(itt->block, itt->row_num))
                        continue;

                    rows_with_block[batch_pos++] = reinterpret_cast<UInt64>(itt.ptr());
                    ++rows_added;
                    if (batch_pos == BATCH_SIZE)
                        populate_col();
                }
            }

            if (batch_pos == BATCH_SIZE)
                populate_col();

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        if (batch_pos > 0)
            populate_col();

        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parent.data->blocks_nullmaps.begin();

        auto end = parent.data->blocks_nullmaps.end();

        for (auto & it = *nulls_position; it != end && rows_added < max_block_size; ++it)
        {
            const Block * block = it->first;
            const NullMap & nullmap = assert_cast<const ColumnUInt8 &>(*it->second).getData();

            for (size_t row = 0; row < nullmap.size(); ++row)
            {
                if (nullmap[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*block->getByPosition(col).column, row);
                    ++rows_added;
                }
            }
        }
    }
};


BlockInputStreamPtr HashJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size, size_t, size_t) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi)
        return {};

    if (isRightOrFull(table_join->kind()))
        return std::make_shared<NonJoinedBlockInputStream>(*this, result_sample_block, max_block_size);
    return {};
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    from_storage_join = true;
    joinDispatch(kind, strictness, data->maps, [this](auto kind_, auto strictness_, auto & map)
    {
        used_flags.reinit<kind_, strictness_>(map.getBufferSizeInCells(data->type) + 1);
    }, has_inequal_condition);
}

static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column))
                column.column = filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure)
{
    LOG_TRACE(log, "({}) Join data is being released, {} bytes and {} rows in hash table", fmt::ptr(this), getTotalByteCount(), getTotalRowCount());

    BlocksList right_blocks = std::move(data->blocks);
    if (!restructure)
    {
        data.reset();
        return right_blocks;
    }

    // todo aron join support "or"
    // todo aron clean this maps
    // data->maps.clear();
    data->blocks_nullmaps.clear();

    BlocksList restored_blocks;

    /// names to positions optimization
    std::vector<size_t> positions;
    std::vector<bool> is_nullable;
    if (!right_blocks.empty())
    {
        positions.reserve(right_sample_block.columns());
        const Block & tmp_block = *right_blocks.begin();
        for (const auto & sample_column : right_sample_block)
        {
            positions.emplace_back(tmp_block.getPositionByName(sample_column.name));
            is_nullable.emplace_back(JoinCommon::isNullable(sample_column.type));
        }
    }

    for (Block & saved_block : right_blocks)
    {
        Block restored_block;
        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto & column = saved_block.getByPosition(positions[i]);
            correctNullabilityInplace(column, is_nullable[i]);
            restored_block.insert(column);
        }
        restored_blocks.emplace_back(std::move(restored_block));
    }

    data.reset();
    return restored_blocks;
}

Block HashJoin::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block structured_block;
    for (const auto & sample_column : saved_block_sample_.getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable2(column);

        if (column.column->lowCardinality() && !sample_column.column->lowCardinality())
        {
            column.column = column.column->convertToFullColumnIfLowCardinality();
            column.type = removeLowCardinality(column.type);
        }

        /// There's no optimization for right side const columns. Remove constness if any.
        // todo aron ColumnSparse
        // column.column = recursiveRemoveSparse(column.column->convertToFullColumnIfConst());
        structured_block.insert(std::move(column));
    }

    return structured_block;
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

bool HashJoin::isEqualNull(const String & name) const
{
    const auto *null_safe_columns = table_join->keyIdsNullSafe();
    if (!null_safe_columns)
        return false;

    for (size_t i = 0; i < key_names_right.size(); ++i)
    {
        if ((*null_safe_columns)[i] && name == key_names_right[i])
            return true;
    }
    return false;
}


template <typename T, bool equal_null>
static bool procNumericBlock(BloomFilterWithRange & bf_with_range, const IColumn * column)
{
    const auto * nullable = checkAndGetColumn<ColumnNullable>(column);

    if (nullable)
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(nullable->getNestedColumn());
        if (!col)
            return false;

        if constexpr (equal_null)
        {
            for (size_t i = 0; i < nullable->size(); ++i)
            {
                if (nullable->isNullAt(i))
                    bf_with_range.addNull();
                else
                    bf_with_range.addKey(col->getData()[i]);
            }
        }
        else
        {
            for (size_t i = 0; i < nullable->size(); ++i)
            {
                if (!nullable->isNullAt(i))
                    bf_with_range.addKey(col->getData()[i]);
            }
        }
    }
    else
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;

        std::for_each(
            col->getData().cbegin(), col->getData().cend(),
            [&](const auto x) { bf_with_range.addKey(x); });
    }

    return true;
}

template<bool equal_null>
static void buildOneBlock(BloomFilterWithRange & bf_with_range, WhichDataType which, const IColumn * column)
{
    bool ret = false;
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) ret = procNumericBlock<TYPE, equal_null>(bf_with_range, column);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        ret = procNumericBlock<UInt16, equal_null>(bf_with_range, column);
    else if (which.idx == TypeIndex::Date32)
        ret = procNumericBlock<Int32, equal_null>(bf_with_range, column);
    else if (which.idx == TypeIndex::DateTime)
        ret = procNumericBlock<UInt32, equal_null>(bf_with_range, column);

    if (!ret)
        throw Exception("buildOneBlock unexpected type of column: " + column->getName(), ErrorCodes::LOGICAL_ERROR);
}

void HashJoin::tryBuildRuntimeFilters() const
{
    auto runtime_filter_consumer = table_join->getRuntimeFilterConsumer();
    if (!runtime_filter_consumer)
        return;

    auto & runtime_filters = runtime_filter_consumer->getRuntimeFilters();
    if (runtime_filters.empty())
        return;

    size_t ht_size = getTotalRowCount();
    if (ht_size == 0 && runtime_filter_consumer->getLocalSteamParallel() == 1)
    {
        bypassRuntimeFilters(BypassType::BYPASS_EMPTY_HT, 0);
        return;
    }

    if (runtime_filter_consumer->getLocalSteamParallel() > 1)
    {
        if (runtime_filter_consumer->isBypassed())
            return; /// already bypassed

        if (runtime_filter_consumer->addBuildParams(ht_size, &data->blocks))
        {
            /// last one, start build
            size_t total_size = runtime_filter_consumer->totalHashTableSize();
            if (total_size > table_join->getInBuildThreshold() && total_size > table_join->getBloomBuildThreshold())
            {
                bypassRuntimeFilters(BypassType::BYPASS_LARGE_HT, total_size);
                return;
            }
            else if (total_size == 0)
            {
                /// edge case all stream is empty
                bypassRuntimeFilters(BypassType::BYPASS_EMPTY_HT, 0);
                return;
            }
            Stopwatch stopwatch;
            std::vector<const BlocksList*> && all_blocks = runtime_filter_consumer->buildParamsBlocks();
            buildAllRF(total_size, all_blocks, runtime_filter_consumer.get());
            runtime_filter_consumer->finalize();
            LOG_TRACE(log, "build rf total rows:{} all cost: {} ms", total_size, stopwatch.elapsedMilliseconds());
            return;
        }

        size_t total_size = runtime_filter_consumer->totalHashTableSize();
        if (!runtime_filter_consumer->isBypassed() && total_size > table_join->getInBuildThreshold() && total_size > table_join->getBloomBuildThreshold())
        {
            bypassRuntimeFilters(BypassType::BYPASS_LARGE_HT, total_size);
            return;
        }
    }
    else
    {
        if (ht_size > table_join->getInBuildThreshold() && ht_size > table_join->getBloomBuildThreshold())
        {
            bypassRuntimeFilters(BypassType::BYPASS_LARGE_HT, ht_size);
            return;
        }
        Stopwatch stopwatch;
        buildAllRF(ht_size, {&data->blocks}, runtime_filter_consumer.get());
        runtime_filter_consumer->finalize();
        LOG_DEBUG(log, "build local rf runtime filter total rows:{} cost: {} ms", ht_size, stopwatch.elapsedMilliseconds());
    }
}

void HashJoin::buildAllRF(size_t total_size, const std::vector<const BlocksList *> & all_blocks, RuntimeFilterConsumer * rf_consumer) const
{
    const auto & runtime_filters = rf_consumer->getRuntimeFilters();
    if (total_size <= table_join->getInBuildThreshold())
    {
        /// only build in filter
        for (const auto & rf : runtime_filters)
        {
            if (!right_table_keys.has(rf.first)) /// shouldn't happen
                continue ;
            buildValueSetRF(rf.second, rf.first, all_blocks, rf_consumer);
        }

        return ;
    }

    if (total_size <= table_join->getBloomBuildThreshold())
    {
        /// just build bloom filter
        for (const auto & rf : runtime_filters)
        {
            if (!right_table_keys.has(rf.first)) /// shouldn't happen
                continue ;

            buildBloomFilterRF(rf.second, rf.first, total_size, all_blocks, rf_consumer);
        }
        return;
    }
}

void HashJoin::buildBloomFilterRF(
    const RuntimeFilterBuildInfos & rf_info, const String & name, size_t ht_size, const std::vector<const BlocksList *> & blocks,
    RuntimeFilterConsumer * rf_consumer) const
{
    bool is_null_safe = isEqualNull(name);
    DataTypePtr type;
    for (const auto & lists : blocks)
    {
        if (lists != nullptr && !lists->empty())
        {
            const auto & col = lists->front().getByName(name);
            type = removeNullable(recursiveRemoveLowCardinality(col.type));
            break;
        }
    }
    WhichDataType which(type);

    /// try enlarge ndv for none-shuffle-aware grf
    size_t pre_enlarge_size = ht_size;
    bool pre_enlarge = true;
    if (rf_consumer->isDistributed(name))
    {
        pre_enlarge_size *= rf_consumer->getParallelWorkers();
        // if (table_join->getShuffleAwareNDVThreshold() && pre_enlarge_size > table_join->getShuffleAwareNDVThreshold())
        // {
        //     pre_enlarge = false;
        //     pre_enlarge_size = ht_size;
        // } // TODO: Yuanning RuntimeFilter
    }

    BloomFilterWithRangePtr bf_with_range
        = std::make_shared<BloomFilterWithRange>(pre_enlarge_size, type);
    bf_with_range->is_pre_enlarged = pre_enlarge;
    for (const auto & lists : blocks)
    {
        if (lists == nullptr || lists->empty())
            continue;
        for (const auto & block : *lists)
        {
            const auto & col = block.getByName(name).column;
            const auto * nullable = checkAndGetColumn<ColumnNullable>(col.get());
            if (nullable)
            {
                const auto * nest_col = nullable->getNestedColumnPtr().get();
                if (nest_col->isNumeric())
                {
                    if (is_null_safe)
                        buildOneBlock<true>(*bf_with_range, which, nullable);
                    else
                        buildOneBlock<false>(*bf_with_range, which, nullable);
                }
                else if (nest_col->getDataType() == TypeIndex::Map || nest_col->getDataType() == TypeIndex::Tuple)
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        if (nullable->isNullAt(i))
                        {
                            if (is_null_safe)
                                bf_with_range->addNull();
                        }
                        else
                        {
                            bf_with_range->addKey<true>(*nest_col, i);
                        }
                    }
                }
                else
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        if (nullable->isNullAt(i))
                        {
                            if (is_null_safe)
                                bf_with_range->addNull();
                        }
                        else
                        {
                            bf_with_range->addKey<false>(*nest_col, i);
                        }
                    }
                }
            }
            else
            {
                if (col->isNumeric())
                {
                    if (is_null_safe)
                        buildOneBlock<true>(*bf_with_range, which, col.get());
                    else
                        buildOneBlock<false>(*bf_with_range, which, col.get());
                }
                else if (col->getDataType() == TypeIndex::Map || col->getDataType() == TypeIndex::Tuple)
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        bf_with_range->addKey<true>(*col, i);
                    }
                }
                else
                {
                    for (size_t i = 0; i < block.rows(); ++i)
                    {
                        bf_with_range->addKey<false>(*col, i);
                    }
                }
            }
        }
    }
    rf_consumer->addFinishRF(std::move(bf_with_range), rf_info.id, rf_info.distribution == RuntimeFilterDistribution::LOCAL);
}

void HashJoin::buildValueSetRF(const RuntimeFilterBuildInfos & rf_info, const String & name, const std::vector<const BlocksList *> & blocks,
                               RuntimeFilterConsumer * rf_consumer) const
{
    DataTypePtr type;
    for (const auto & lists : blocks)
    {
        if (lists != nullptr && !lists->empty())
        {
            const auto & col = lists->front().getByName(name);
            type = removeNullable(recursiveRemoveLowCardinality(col.type));
            break;
        }
    }

    ValueSetWithRangePtr vs_with_range = std::make_shared<ValueSetWithRange>(type);
    for (const auto & lists : blocks)
    {
        if (lists == nullptr || lists->empty())
            continue;
        for (const auto & block : *lists)
        {
            const auto & col = block.getByName(name).column;
            Field field;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                col->get(i, field);
                if (!field.isNull())
                    vs_with_range->insert(field);
            }
        }
    }
    rf_consumer->addFinishRF(std::move(vs_with_range), rf_info.id, rf_info.distribution == RuntimeFilterDistribution::LOCAL);
}

void HashJoin::bypassRuntimeFilters(BypassType type, size_t total_size) const
{
    const auto & runtime_filter_consumer = table_join->getRuntimeFilterConsumer();
    if (!runtime_filter_consumer)
        return;
    LOG_DEBUG(log, "going build rf bypass rf: {}, size:{}", bypassTypeToString(type), total_size);
    runtime_filter_consumer->bypass(type);
}

void HashJoin::validateInequalConditions(const ExpressionActionsPtr & inequal_condition_actions_)
{
    Block expression_sample_block = inequal_condition_actions_->getSampleBlock();

    size_t column_size = expression_sample_block.columns();
    auto filter_type = expression_sample_block.getByPosition(column_size - 1).type;

    if (!isUInt8(filter_type)
        && !(filter_type->isNullable() && isUInt8(typeid_cast<const DataTypeNullable &>(*filter_type).getNestedType())))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got '{}'",
            filter_type->getName());
    }

    bool is_supported = (strictness == ASTTableJoin::Strictness::All || strictness == ASTTableJoin::Strictness::Semi || strictness == ASTTableJoin::Strictness::Anti)
                        && (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Right)
                        && table_join->oneDisjunct();

    if (!is_supported)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Non equi condition '{}' from JOIN ON section is supported only for (ALL / Semi / Anti) RIGHT and LEFT JOINs, get {} {} JOIN",
            expression_sample_block.getByPosition(column_size - 1).name, strictnessToString(strictness), kindToString(kind));
    }
    has_inequal_condition = true;
    LOG_DEBUG(getLogger("HashJoin"), "validate inequal condition for header: {}", expression_sample_block.dumpStructure());
}

}
