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

#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <array>
#include <stack>
#include <optional>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeBitMap64.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnBitMap64.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <common/logger_useful.h>
#include <Common/ArenaAllocator.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>
#include <shared_mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum LogicOperationType
{
    NONE, // Maybe only one bitmap column in the join
    AND,
    OR,
    XOR,
    ANDNOT,
    REVERSEANDNOT,
    UNDEFINED // It is not a true type, and should always put at the last one
};

struct LogicOperation
{
    LogicOperation() : logic_op(LogicOperationType::NONE) {}
    explicit LogicOperation(String operation)
    {
        std::transform(operation.begin(), operation.end(), operation.begin(), ::toupper);
        if (operation == "NONE" || operation.empty())
            logic_op = LogicOperationType::NONE;
        else if (operation == "AND")
            logic_op = LogicOperationType::AND;
        else if (operation == "OR")
            logic_op = LogicOperationType::OR;
        else if (operation == "XOR")
            logic_op = LogicOperationType::XOR;
        else if (operation == "ANDNOT")
            logic_op = LogicOperationType::ANDNOT;
        else if (operation == "RANDNOT" || operation == "REVERSEANDNOT")
            logic_op = LogicOperationType::REVERSEANDNOT;
        else
            logic_op = LogicOperationType::UNDEFINED;
    }

    LogicOperation(const LogicOperation & rhs)
    {
        this->logic_op = rhs.logic_op;
    }

    bool isValid() const { return logic_op < LogicOperationType::UNDEFINED; }

    LogicOperationType logic_op;
};

enum JoinType
{
    INNER,
    LEFT,
    INVALID
};

struct JoinOperation
{
    JoinOperation() : join_op(JoinType::INNER) {}
    explicit JoinOperation(String operation)
    {
        std::transform(operation.begin(), operation.end(), operation.begin(), ::toupper);
        if (operation.empty() || operation == "INNER")
            join_op = JoinType::INNER;
        else if (operation == "LEFT")
            join_op = JoinType::LEFT;
        else
            join_op = JoinType::INVALID;
    }

    bool isValid() const { return join_op < JoinType::INVALID; }

    JoinType join_op;
};

using JoinKeys = Strings;
using GroupByKeys = Strings;
using Position = UInt8;
using BitMapPtr = std::shared_ptr<BitMap64>;
using JoinKeysPtr = std::shared_ptr<JoinKeys>;
using GroupByKeysPtr = std::shared_ptr<GroupByKeys>;
using JoinTuple = std::tuple<JoinKeysPtr, GroupByKeysPtr, BitMapPtr>;
using JoinTuplePtr = std::shared_ptr<JoinTuple>;
using JoinTuplePtrs = std::vector<JoinTuplePtr>;
using PositionIndexPair = std::pair<UInt64, UInt64>;

void writeStrings(const std::shared_ptr<Strings> & data, WriteBuffer & buf)
{
    size_t size = data->size();
    writeVarUInt(size, buf);
    for (auto & key : *data)
        writeString(key.data(), key.size(), buf);
}

void readStrings(Strings & data, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        String key;
        readString(key, buf);
        data.emplace_back(key);
    }
}

// The key used to hash the join keys or group by keys
struct StringsMapKey
{
    std::shared_ptr<Strings> keys;

    StringsMapKey() = default;
    explicit StringsMapKey(String & key_)
    {
        Strings strs{ key_ };
        keys = std::make_shared<Strings>(std::move(strs));
    }
    explicit StringsMapKey(std::shared_ptr<Strings> && keyPtr) : keys(std::move(keyPtr)) {}

    bool operator==(const StringsMapKey & rhs) const
    {
        if (keys->size() != rhs.keys->size())
            return false;

        for (size_t i = 0; i < keys->size(); ++i)
        {
            if (keys->at(i) != rhs.keys->at(i))
                return false;
        }
        return true;
    }
};

struct HashStringsMapKey
{
    size_t operator()(const StringsMapKey & one) const
    {
        if (one.keys->empty())
            return std::hash<String>()("");

        size_t res = std::hash<String>()(one.keys->at(0));
        for (size_t i = 1; i < one.keys->size(); ++i)
            res ^= std::hash<String>()(one.keys->at(i)) >> i;

        return res;
    }
};

using HashedStringsKeyTuples = std::unordered_map<StringsMapKey, JoinTuplePtrs, HashStringsMapKey>;
using Pairs = std::vector<std::pair<StringsMapKey, JoinTuplePtrs>>;

// A data structure contains a key_tuples map and a lock
// It is used in KVSharded
class KVBigLock {
public:

    void emplace(const StringsMapKey && key, const JoinTuplePtrs && value)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_map.emplace(std::move(key), std::move(value));
    }

    void emplaceKVOrAddValue(const StringsMapKey && key, const JoinTuplePtrs && value)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_map.find(key);
        if (it == m_map.end())
            m_map.emplace(std::move(key), std::move(value));
        else
        {
            *(std::get<2>(*(m_map[key].front()))) |= *(std::get<2>(*(value.at(0))));
        }
    }

    std::optional<JoinTuplePtrs> get(const StringsMapKey & key)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_map.find(key);
        if (it != m_map.end())
            return it->second;   // is it->second empty?
        return {};
    }

    // Here is no lock, we just do this in a single thread
    void getAllKeyValueByResultType(ColumnTuple & tuple_in_array, size_t result_type)
    {
        for (auto & it : m_map)
        {
            BitMapPtr bitmap_ptr = std::get<2>(*(it.second.at(0)));
            size_t key_size = it.first.keys->size();
            for (size_t i = 0; i < key_size; ++i)
            {
                auto & column_group_by = static_cast<ColumnString &>(tuple_in_array.getColumn(i));
                column_group_by.insert(it.first.keys->at(i));
            }
            if (result_type == 0)
            {
                auto & column_card = static_cast<ColumnUInt64 &>(tuple_in_array.getColumn(key_size));
                column_card.insert(bitmap_ptr->cardinality());
            }
            else if (result_type == 1)
            {
                auto & column_bitmap = static_cast<ColumnBitMap64 &>(tuple_in_array.getColumn(key_size));
                column_bitmap.insert(*bitmap_ptr);
            }
        }
    }

private:
    std::mutex m_mutex;
    HashedStringsKeyTuples m_map;
};

// This is a data structure contains serveral map with segment lock to reduce competition
class KVSharded
{
public:
    explicit KVSharded(size_t num_shard) : m_mask(num_shard - 1), m_shards(num_shard)
    {
        if ((num_shard & m_mask) != 0)
            throw Exception("num_shard should be a power of two", ErrorCodes::BAD_ARGUMENTS);
    }

    KVSharded(KVSharded && rhs) : m_mask(std::move(rhs.m_mask)), m_shards(std::move(rhs.m_shards)) {}
    KVSharded& operator=(KVSharded && rhs)
    {
        if (this != &rhs) // Optional: Check for self-assignment
        {
            m_mask = std::move(rhs.m_mask);
            m_shards = std::move(rhs.m_shards);
        }
        return *this;
    }

    void put(const StringsMapKey & key, const JoinTuplePtrs & value)
    {
        getShard(key).emplaceKVOrAddValue(std::move(key), std::move(value));
    }

    std::optional<JoinTuplePtrs> get(const StringsMapKey & key)
    {
        return getShard(key).get(key);
    }

    /// It's used in insertIntoResult function, by a single thread
    void writeResultOfKeyAndValue(ColumnTuple & tuple_in_array, size_t result_type)
    {
        for (auto & m_shard : m_shards)
        {
            m_shard.getAllKeyValueByResultType(tuple_in_array, result_type);
        }
    }

private:
    size_t m_mask;
    std::vector<KVBigLock> m_shards;

    KVBigLock & getShard(const StringsMapKey & key)
    {
        HashStringsMapKey hash_fn;
        size_t h = hash_fn(key);
        return m_shards[h & m_mask];
    }
};

/// It's used to accommodate user input data, and data is grouped by join keys
struct JoinPositionTuples
{
    Position position;
    HashedStringsKeyTuples tuples;   // The key used here is join key

    JoinPositionTuples() = default;
    explicit JoinPositionTuples(Position pos) : position(pos) {}
    JoinPositionTuples(const JoinPositionTuples & rhs) = default;
    JoinPositionTuples(JoinPositionTuples && rhs) : position(rhs.position), tuples(std::move(rhs.tuples)) {}
    JoinPositionTuples(Position && pos, StringsMapKey && join_keys, JoinTuplePtr && val)
        : position(std::move(pos)), tuples{{std::move(join_keys), JoinTuplePtrs{val}}} {}

    JoinPositionTuples& operator=(const JoinPositionTuples & rhs)
    {
        if (this != &rhs) { // Check for self-assignment
            this->position = rhs.position;
            this->tuples = rhs.tuples;
        }
        return *this;
    }

    JoinPositionTuples& operator=(JoinPositionTuples && rhs)
    {
        if (this != &rhs) { // Check for self-assignment
            this->position = std::move(rhs.position);
            this->tuples = std::move(rhs.tuples);
        }
        return *this;
    }

    void emplaceBack(StringsMapKey && join_key, JoinTuplePtrs && value)
    {
        auto it = this->tuples.find(join_key);
        if (it == this->tuples.end())
        {
            this->tuples.emplace(std::move(join_key), std::move(value));
        }
        else
            it->second.insert(it->second.end(),
                              std::make_move_iterator(value.begin()),
                              std::make_move_iterator(value.end()));
    }

    void emplaceBack(StringsMapKey && join_key, JoinTuplePtr && value)
    {
        this->emplaceBack(std::move(join_key), JoinTuplePtrs{value});
    }

    void insert(JoinPositionTuples && rhs)
    {
        for (auto & tuple : rhs.tuples)
        {
            this->emplaceBack(std::move(const_cast<StringsMapKey &>(tuple.first)), std::move(tuple.second));
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(position, buf);
        size_t map_size = tuples.size();
        writeVarUInt(map_size, buf);
        for (const auto & tuple : tuples)
        {
            writeStrings(tuple.first.keys, buf);

            size_t tuples_num = tuple.second.size();
            writeVarUInt(tuples_num, buf);
            for (const auto & jt : tuple.second)
            {
                JoinKeysPtr join_key_ptr;
                GroupByKeysPtr group_by_ptr;
                BitMapPtr bitmap_ptr;
                std::tie(join_key_ptr, group_by_ptr, bitmap_ptr) = *jt;

                writeStrings(const_cast<const JoinKeysPtr&>(join_key_ptr), buf);
                writeStrings(const_cast<const GroupByKeysPtr&>(group_by_ptr), buf);

                size_t bytes_size = (*bitmap_ptr).getSizeInBytes();
                writeVarUInt(bytes_size, buf);
                PODArray<char> buffer(bytes_size);
                (*bitmap_ptr).write(buffer.data());
                writeString(buffer.data(), bytes_size, buf);
            }
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t pos = 0;
        readVarUInt(pos, buf);
        this->position = static_cast<Position>(pos);

        size_t map_size = 0;
        readVarUInt(map_size, buf);

        for (size_t i = 0; i < map_size; ++i)
        {
            Strings key;
            readStrings(key, buf);

            JoinTuple tmp_tuple;
            JoinTuplePtrs tuples_ptrs;

            size_t tuples_num = 0;
            readVarUInt(tuples_num, buf);

            for (size_t j = 0; j < tuples_num; ++j)
            {
                JoinKeys join_key;
                GroupByKeys group_by;

                readStrings(join_key, buf);
                readStrings(group_by, buf);

                size_t bytes_size;
                readVarUInt(bytes_size, buf);
                PODArray<char> buffer(bytes_size);
                buf.readStrict(buffer.data(), bytes_size);
                BitMap64 bitmap = BitMap64::readSafe(buffer.data(), bytes_size);

                JoinKeysPtr join_key_ptr = make_shared<JoinKeys>(join_key);
                GroupByKeysPtr group_by_ptr = make_shared<GroupByKeys>(group_by);

                tmp_tuple = std::make_tuple(std::move(join_key_ptr),
                                            std::move(group_by_ptr),
                                            std::make_shared<BitMap64>(bitmap));

                tuples_ptrs.emplace_back(std::make_shared<JoinTuple>(tmp_tuple));
            }

            std::shared_ptr<Strings> key_ptr = std::make_shared<Strings>(std::move(key));
            this->emplaceBack(StringsMapKey(std::move(key_ptr)), std::move(tuples_ptrs));
        }
    }
};

struct AggregateFunctionBitMapJoinData
{
    AggregateFunctionBitMapJoinData() = default;

    std::vector<JoinPositionTuples> join_tuples_by_position;

    void add(const Position & pos, const BitMapPtr bitmap_ptr, JoinKeysPtr & join_keys, GroupByKeysPtr & group_bys, size_t union_num)
    {
        if (pos > union_num+1)
            throw Exception("AggregateFunction BitMapJoin: Wrong position value. Position starts from 1 and ends with join_num+1 ",
                            DB::ErrorCodes::BAD_ARGUMENTS);

        JoinKeysPtr cpy(join_keys);
        StringsMapKey key(std::move(cpy));
        JoinTuplePtr tuple_ptr{std::make_shared<JoinTuple>(std::make_tuple(std::move(join_keys), std::move(group_bys), std::move(bitmap_ptr)))};

        for (auto & pos_tuples : join_tuples_by_position) // Position value is in a small range, just compare one by one
        {
            if (pos-1 == pos_tuples.position)  // position starts from 0, but pos from user starts from 1
            {
                pos_tuples.emplaceBack(std::move(key), std::move(tuple_ptr));
                return;
            }
        }

        join_tuples_by_position.emplace_back(std::move(pos-1), std::move(key), std::move(tuple_ptr));
    }

    void merge (const AggregateFunctionBitMapJoinData & rhs)
    {
        auto & lhs_tuples_by_position = this->join_tuples_by_position;
        auto & rhs_tuples_by_position = const_cast<std::vector<JoinPositionTuples> &>(rhs.join_tuples_by_position);

        if (rhs_tuples_by_position.empty())
            return;
        else if (lhs_tuples_by_position.empty())
        {
            lhs_tuples_by_position = std::move(rhs_tuples_by_position);
            return;
        }

        // Position value is in a small range, just compare one by one
        for (auto & rt : rhs_tuples_by_position)
        {
            bool pos_exists = false;
            for (auto & lt : lhs_tuples_by_position)
            {
                if (lt.position == rt.position)
                {
                    lt.insert(std::move(rt));
                    pos_exists = true;
                }
            }
            if (!pos_exists)
            {
                lhs_tuples_by_position.emplace_back(std::move(rt));
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t position_num = join_tuples_by_position.size();
        writeVarUInt(position_num, buf);
        for (const auto & it : join_tuples_by_position)
        {
            it.serialize(buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t position_num = 0;
        readVarUInt(position_num, buf);

        for (size_t i = 0; i < position_num; ++i)
        {
            JoinPositionTuples pos_tuple;
            pos_tuple.deserialize(buf);
            join_tuples_by_position.emplace_back(std::move(pos_tuple));
        }
    }
};

class AggregateFunctionBitMapJoin final : public IAggregateFunctionDataHelper<AggregateFunctionBitMapJoinData, AggregateFunctionBitMapJoin>
{
public:
    AggregateFunctionBitMapJoin(const DataTypes & argument_types_, size_t union_num_, std::vector<PositionIndexPair> join_keys_idx_,
                                std::vector<PositionIndexPair> group_by_keys_idx_, LogicOperation logic_op, JoinOperation join_op, UInt64 thread_num_, UInt64 result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionBitMapJoinData, AggregateFunctionBitMapJoin>(argument_types_, {}),
          union_num(union_num_), arguments_num(argument_types_.size()), join_keys_idx(join_keys_idx_), group_by_keys_idx(group_by_keys_idx_),
          logic_operation(logic_op), join_operation(join_op), thread_num(thread_num_), result_type(result_type_) {}

    String getName() const override { return "bitmapJoin"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        for (size_t i = 0; i < group_by_keys_idx.size(); ++i)
            types.emplace_back(std::make_shared<DataTypeString>()); // group by
        if (result_type == 0)
            types.emplace_back(std::make_shared<DataTypeUInt64>());  // cardinality
        else if (result_type == 1)
            types.emplace_back(std::make_shared<DataTypeBitMap64>()); // raw bitmap

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto * column_position = checkAndGetColumn<ColumnUInt8>(columns[0]);
        Position pos = column_position->getElement(row_num);

        const auto * column_bitmap = checkAndGetColumn<ColumnBitMap64>(columns[1]);
        const BitMap64 & bitmap = column_bitmap->getBitMapAt(row_num);
        auto bitmap_ptr = std::make_shared<BitMap64>(std::move(const_cast<BitMap64 &>(bitmap)));

        Strings columns_str;
        for (size_t i = 2; i < arguments_num; ++i)
        {
            const auto * column_arg = checkAndGetColumn<ColumnString>(columns[i]); // args start from columns[4]
            columns_str.emplace_back(column_arg->getDataAt(row_num).toString());
        }

        JoinKeys join_keys;
        GroupByKeys group_by_keys;
        for (auto pi : join_keys_idx)
        {
            // join key starts from 3 in user's input, and it appears in each position
            join_keys.emplace_back(columns_str.at(pi.second - 3));
        }

        for (auto pi : group_by_keys_idx)
        {
            if (pi.first == static_cast<UInt64>(pos) && columns_str.at(pi.second - 3) == "#-1#")
                throw Exception("The column you identified for group by is invalid, where data is '#-1#'", ErrorCodes::BAD_ARGUMENTS);

            group_by_keys.emplace_back(columns_str.at(pi.second - 3));
        }

        auto join_keys_ptr = make_shared<JoinKeys>(join_keys);
        auto group_by_keys_ptr = make_shared<GroupByKeys>(group_by_keys);

        this->data(place).add(pos, bitmap_ptr, join_keys_ptr, group_by_keys_ptr, union_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & this_join_tuples = const_cast<std::vector<JoinPositionTuples> &>(this->data(place).join_tuples_by_position);
        if (this_join_tuples.size() < 2)
            return;

        sort(this_join_tuples.begin(), this_join_tuples.end(),
            [](const JoinPositionTuples & left, const JoinPositionTuples & right) -> bool {
                return left.position < right.position;
            });

        auto result_group_by_tuples = doJoinWithLogicOperation(this_join_tuples);

        auto & column_res = static_cast<ColumnArray &>(to);
        auto & column_offsets = static_cast<ColumnArray::ColumnOffsets &>(column_res.getOffsetsColumn());

        auto & tuple_in_array = static_cast<ColumnTuple &>(column_res.getData());

        // insert result to res_column
        result_group_by_tuples.writeResultOfKeyAndValue(tuple_in_array, result_type);

        column_offsets.getData().push_back(column_res.getData().size());
    }

private:
    void joinMultiThreads(KVSharded & result,
                          std::vector<Pairs> & split_lhs_data,
                          HashedStringsKeyTuples & rhs_data,
                          size_t thread_num_) const
    {
        ThreadGroupStatusPtr thread_group = CurrentThread::getGroup();

        auto run_join = [&](size_t index)
        {
            setThreadName("bitmapJoin");
            CurrentThread::attachToIfDetached(thread_group);

            JoinTuplePtrs tuples_tmp;
            Pairs & group = split_lhs_data.at(index);
            for (auto & gt : group)
            {
                auto & key = gt.first;
                auto & left = gt.second; // left JoinTuplePtrs

                if (left.empty())
                    continue;

                auto rjt = rhs_data.find(key);
                if (rjt == rhs_data.end()) // key is not matched
                {
                    switch (join_operation.join_op)
                    {
                        case JoinType::INNER : // INNER JOIN
                            continue;
                        case JoinType::LEFT :   // ALL LEFT JOIN
                            {
                                for (auto & it : left)
                                {
                                    auto group_by_keys = std::get<1>(*it);
                                    result.put(StringsMapKey(std::move(group_by_keys)), {it});
                                }
                            }
                            continue;
                        default:
                            break;
                    }
                }

                auto & right = rjt->second;  // right JoinTuplePtrs
                for (auto & lt : left)
                {
                    for (const auto & rt : right)
                    {
                        JoinKeysPtr join_keys_ptr;
                        GroupByKeysPtr lt_group_bys, rt_group_bys;
                        BitMapPtr lt_bitmap_ptr, rt_bitmap_ptr;

                        std::tie(join_keys_ptr, lt_group_bys, lt_bitmap_ptr) = *lt;
                        std::tie(std::ignore, rt_group_bys, rt_bitmap_ptr) = *rt;

                        Strings group_bys;
                        for (size_t i = 0; i < group_by_keys_idx.size(); ++i)
                        {
                            if (group_by_keys_idx[i].first == 0xFF)  // If no position identifier
                            {
                                if (lt_group_bys->at(i) != "#-1#") // left subquery has a group by key
                                    group_bys.emplace_back(std::move(lt_group_bys->at(i)));
                                else
                                    group_bys.emplace_back(std::move(rt_group_bys->at(i)));
                            }
                            else
                            {
                                if (group_by_keys_idx[i].first == 1)
                                    group_bys.emplace_back(std::move(lt_group_bys->at(i)));
                                else if (group_by_keys_idx[i].first == 2)
                                    group_bys.emplace_back(std::move(rt_group_bys->at(i)));
                            }
                        }

                        BitMap64 bitmap(*lt_bitmap_ptr);

                        switch (logic_operation.logic_op)
                        {
                            case DB::LogicOperationType::NONE :
                            {
                                if (lt_bitmap_ptr->isEmpty())
                                    bitmap = *rt_bitmap_ptr;
                            }
                            break;
                            case DB::LogicOperationType::AND :
                                bitmap &= *rt_bitmap_ptr;
                                break;
                            case DB::LogicOperationType::OR :
                                bitmap |= *rt_bitmap_ptr;
                                break;
                            case DB::LogicOperationType::XOR :
                                bitmap ^= *rt_bitmap_ptr;
                                break;
                            case DB::LogicOperationType::ANDNOT :
                                bitmap -= *rt_bitmap_ptr;
                                break;
                            case DB::LogicOperationType::REVERSEANDNOT :
                                bitmap = *rt_bitmap_ptr - bitmap;
                                break;
                            default:
                                break;
                        }

                        auto group_by_ptr = make_shared<GroupByKeys>(group_bys);

                        JoinTuple tmp_tuple{std::make_tuple(join_keys_ptr, group_by_ptr,
                                            std::make_shared<BitMap64>(std::move(bitmap)))};

                        result.put(std::move(StringsMapKey(std::move(group_by_ptr))),
                                   std::move(JoinTuplePtrs{std::make_shared<JoinTuple>(tmp_tuple)}));
                   }
                }
               left.clear();  // destruct bitmap in multi-thread
            }
        };

        std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(thread_num_);

        for (size_t i = 0; i < thread_num_; ++i)
        {
            auto join_and_func = [i, &run_join]() { run_join(i); };
            thread_pool->scheduleOrThrowOnError(join_and_func);
        }

        thread_pool->wait();
    }

    KVSharded doJoinWithLogicOperation(std::vector<JoinPositionTuples> & this_join_tuples) const
    {
        HashedStringsKeyTuples & left_join_tuples = this_join_tuples.at(0).tuples;
        HashedStringsKeyTuples & right_join_tuples = this_join_tuples.at(1).tuples;

        // split the map to several vector
        std::vector<Pairs> pair_vector_buckets;
        size_t idx = 0;
        auto key_tuple_it = left_join_tuples.begin();
        for (; key_tuple_it != left_join_tuples.end(); ++key_tuple_it)
        {
            Pairs p{{key_tuple_it->first, key_tuple_it->second}};
            pair_vector_buckets.emplace_back(p);
            ++idx;
        }

        /// processing remaing data
        for (; key_tuple_it != left_join_tuples.end(); ++key_tuple_it)
        {
            pair_vector_buckets.at(idx % thread_num).emplace_back(key_tuple_it->first, key_tuple_it->second);
            ++idx;
        }
        left_join_tuples.clear();

        KVSharded result(128);
        size_t actual_thread_num = std::min(thread_num, pair_vector_buckets.size());
        joinMultiThreads(result, pair_vector_buckets, right_join_tuples, actual_thread_num);

        return result;
    }

    size_t union_num;
    size_t arguments_num;
    std::vector<PositionIndexPair> join_keys_idx;
    std::vector<PositionIndexPair> group_by_keys_idx;
    LogicOperation logic_operation;
    JoinOperation join_operation;
    size_t thread_num;
    size_t result_type;
};
}
