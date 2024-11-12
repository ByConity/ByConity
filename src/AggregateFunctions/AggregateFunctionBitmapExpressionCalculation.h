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

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnBitMap64.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include "AggregateBitmapExpressionCommon.h"
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
struct AggregateFunctionBitMapCountData
{
    AggregateFunctionBitMapData<T> bitmap_data;
    UInt64 count = 0;

    void merge(AggregateFunctionBitMapCountData<T> & rhs)
    {
        if (bitmap_data.empty())
        {
            bitmap_data = std::move(rhs.bitmap_data);
            count = rhs.count;
        }
        else
        {
            bitmap_data.merge(std::move(rhs.bitmap_data));
        }
    }

    void serialize(WriteBuffer & buf)
    {
        bitmap_data.serialize(buf);
        writeVarUInt(count, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        bitmap_data.deserialize(buf);
        readVarUInt(count, buf);
    }
};

template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
struct AggregateFunctionBitMapMultiCountData
{
    AggregateFunctionBitMapData<T> bitmap_data;
    std::vector<UInt64> count_vector;

    void merge(AggregateFunctionBitMapMultiCountData<T> & rhs)
    {
        if (bitmap_data.empty())
        {
            bitmap_data = std::move(rhs.bitmap_data);
            count_vector = std::move(rhs.count_vector);
        }
        else
        {
            bitmap_data.merge(std::move(rhs.bitmap_data));
        }
    }

    void serialize(WriteBuffer & buf)
    {
        bitmap_data.serialize(buf);
        writeVarUInt(count_vector.size(), buf);
        for (auto cnt : count_vector)
            writeVarUInt(cnt, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        bitmap_data.deserialize(buf);
        size_t vector_size;
        readVarUInt(vector_size, buf);
        for (size_t i = 0; i < vector_size; ++i)
        {
            UInt64 temp_count;
            readVarUInt(temp_count, buf);
            count_vector.emplace_back(temp_count);
        }
    }
};



template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
struct AggregateFunctionBitMapExtractData
{
    AggregateFunctionBitMapData<T> bitmap_data;

    void merge(AggregateFunctionBitMapExtractData<T> & rhs)
    {
        if (bitmap_data.empty())
        {
            bitmap_data = std::move(rhs.bitmap_data);
        }
        else
        {
            bitmap_data.merge(std::move(rhs.bitmap_data));
        }
    }

    void serialize(WriteBuffer & buf)
    {
        bitmap_data.serialize(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        bitmap_data.deserialize(buf);
    }
};

template<typename T>
inline T get_bitmap_key_from_column(const IColumn * column, int row_num)
{
    return static_cast<T>(column->getInt(row_num));
}

template<>
inline String get_bitmap_key_from_column(const IColumn * column, int row_num)
{
    if (const auto * column_string = typeid_cast<const ColumnString*>(column))
    {
        return column_string->getDataAt(row_num).toString();
    }
    else
    {
        throw Exception("In get_bitmap_key_from_column(), typeid_cast failed", ErrorCodes::BAD_ARGUMENTS);
    }
}

template<typename T>
inline String get_bitmap_string_key_from_column(const IColumn * column, int row_num)
{
    return std::to_string(column->getUInt(row_num));
}

template<>
inline String get_bitmap_string_key_from_column<String>(const IColumn * column, int row_num)
{
    if (const auto * column_string = typeid_cast<const ColumnString*>(column))
    {
        return column_string->getDataAt(row_num).toString();
    }
    else
    {
        throw Exception("In get_bitmap_string_key_from_column(), typeid_cast failed", ErrorCodes::BAD_ARGUMENTS);
    }
}

/// Simply count number of calls.
template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapCount final : public IAggregateFunctionDataHelper<AggregateFunctionBitMapCountData<T>, AggregateFunctionBitMapCount<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
private:
    BitMapExpressionAnalyzer<T> analyzer;
    T final_key;
    UInt64 is_bitmap_execute = false;
public:
    AggregateFunctionBitMapCount(const DataTypes & argument_types_, String expression_, UInt64 is_bitmap_execute_)
     : IAggregateFunctionDataHelper<AggregateFunctionBitMapCountData<T>, AggregateFunctionBitMapCount<T>>(argument_types_, {})
     , analyzer(expression_), is_bitmap_execute(is_bitmap_execute_)
    {
        final_key = analyzer.final_key;
    }

    String getName() const override { return "BitmapCount"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        T key = get_bitmap_key_from_column<T>(columns[0], row_num);

        if (check_is_internal_bitmap_key(key))
            throw Exception(
                fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", key)
                , ErrorCodes::BAD_ARGUMENTS
            );

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        const auto & column_bitmap = static_cast<const ColumnBitMap64 &>(*columns[1]);

        const BitMap64 & bitmap = column_bitmap.getBitMapAt(row_num);

        auto & bitmap_data = this->data(place).bitmap_data;

        bitmap_data.add(key, bitmap);
    }

    bool mergeSingleStream(AggregateFunctionBitMapCountData<T> & bitmap_count_data) const
    {
        auto & bitmap_data = bitmap_count_data.bitmap_data;
        auto & count = bitmap_count_data.count;

        if (bitmap_data.is_finished)
            return false;

        analyzer.executeExpression(bitmap_data);
        count = bitmap_data.getCardinality(final_key);
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;

        auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
        auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
        bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
        bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();

        if (lhs_bitmap_exists && rhs_bitmap_exists)
        {
            lhs_bitmap_it->second |= rhs_bitmap_it->second;
        }
        else
            throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::LOGICAL_ERROR);
    }

    /*
    * The merge function is based on the fact different stream can calculate its result independently.
    * If we use BITMAPEXECUTE, ParallelBitMapBlockInputStream will produce many streams and invoke this merge
    * function for each stream. And only the ParallelBitMapBlockInputStream can invoke merge with only one stream, so
    * we can calculate expression for this single stream directly and set is_final as true.
    * When there are two streams, it may be invoked by ParallelBitMapBlockInputStream or normal aggregating stream. But,
    * if is_finished is true for both two streams, we can ensure these two streams can calculate directly.
    * Otherwise, handle them as normal aggregate functions, that is, use bitmap or to get all bitmap and calculate expression
    * in insertResultInto functions
    *
    * For distributed queries, we also have two merge model:
    * 1. distributed_perfect_shard: each node performs the merge as single node, the final result is merged by sum or bitmapOr function.
    * 2. distributed table: each node performs the aggregation at local and return a intermediate status, then at the coordinator node, it performs a
    * second aggregation. There exists the case the second aggregation has multiple merges - the first merge has single stream, and then merge the following
    * streams.
    *
    * The second case will obstruct the determine of BITMAPEXECUTE model, since for a merge with only one stream, we cannot distinguish whether it is a BITMAPEXECUTE model
    * or distributed table. The only thing we can do is assuming this stream can be used to execute expression.
    * This assumption is correct as follows.
    * 1. If it is a distributed table and it has only one stream, execute expression in advanced will not affect the correctness of the result.
    *    If it has more than one stream, we lost some performance to execute first stream's expression. The first stream will merge other streams again and set is_finished as false.
    *    Then calculate the result in the insertResultInto function.
    * 2. If it is a BITMAPEXECUTE model, each stream will be passed to merge function independently in ParallelBitMapInputStream so that each stream will set is_finished as true.
    *
    */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapCountData<T> &>(this->data(rhs));

        if (is_bitmap_execute && lhs_data.bitmap_data.empty())
        {
            mergeSingleStream(rhs_data);
            lhs_data.merge(rhs_data);
        }
        else if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            lhs_data.count += rhs_data.count;
        }
        // If two stream are both false on is_finished, it must not a BITMAPEXECUTE model, so merge them and delay expression execution on insertResultInto
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
        // If one of stream's is_finished is false, it means
        // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
        // 2. it may be execution on distributed table
        // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapCountData<T> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }

        static_cast<ColumnUInt64 &>(to).getData().push_back(local_data.count);
    }

};

/// Simply count number of calls.
template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapMultiCount final : public IAggregateFunctionDataHelper<AggregateFunctionBitMapMultiCountData<T>, AggregateFunctionBitMapMultiCount<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
private:
    BitMapExpressionMultiAnalyzer<T> analyzer;
    std::vector<T> final_keys;
public:
    AggregateFunctionBitMapMultiCount(const DataTypes & argument_types_, std::vector<String> expression_)
            : IAggregateFunctionDataHelper<AggregateFunctionBitMapMultiCountData<T>, AggregateFunctionBitMapMultiCount<T>>(argument_types_, {}), analyzer(expression_)
    {
        final_keys = analyzer.final_keys;
    }

    String getName() const override { return "BitmapMultiCount"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        T key = get_bitmap_key_from_column<T>(columns[0], row_num);

        if (check_is_internal_bitmap_key(key))
            throw Exception(
                fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", key)
                , ErrorCodes::BAD_ARGUMENTS
            );

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        const auto & column_bitmap = static_cast<const ColumnBitMap64 &>(*columns[1]);

        const BitMap64 & bitmap = column_bitmap.getBitMapAt(row_num);

        auto & bitmap_data = this->data(place).bitmap_data;

        bitmap_data.add(key, bitmap);
    }

    bool mergeSingleStream(AggregateFunctionBitMapMultiCountData<T> & bitmap_count_data) const
    {
        auto & bitmap_data = bitmap_count_data.bitmap_data;
        auto & count_vector = bitmap_count_data.count_vector;

        if (bitmap_data.is_finished)
            return false;
        count_vector.clear();
        for (size_t i = 0; i < final_keys.size(); i++)
        {
            analyzer.executeExpression(bitmap_data, i);
            count_vector.push_back(bitmap_data.getCardinality(final_keys[i]));
        }
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;
        for (auto& final_key: final_keys)
        {
            auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
            auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
            bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
            bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();
            if (lhs_bitmap_exists && rhs_bitmap_exists)
            {
                lhs_bitmap_it->second |= rhs_bitmap_it->second;
            }
            else
                throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    /*
    * The merge function is based on the fact different stream can caculate its result independently.
    * If we use BITMAPEXECUTE, ParallelBitMapBlockInputStream will produce many streams and invoke this merge
    * function for each stream. And only the ParallelBitMapBlockInputStream can invoke merge with only one stream, so
    * we can caculate expression for this single stream directly and set is_final as true.
    * When there are two streams, it may be invoked by ParallelBitMapBlockInputStream or normal aggregating stream. But,
    * if is_finished is true for both two streams, we can ensure these two streams can caculate directly.
    * Otherwise, handle them as normal aggregate functions, that is, use bitmap or to get all bitmap and caculate expression
    * in insertResultInto functions
    *
    * For distributed queries, we also have two merge model:
    * 1. distributed_perfect_shard: each node performs the merge as single node, the final result is merged by sum or bitmapOr function.
    * 2. distributed table: each node performs the aggregation at local and return a intermediate status, then at the coordinator node, it performs a
    * second aggregation. There exists the case the second aggregation has multiple merges - the first merge has single stream, and then merge the following
    * streams.
    *
    * The second case will obstruct the determine of BITMAPEXECUTE model, since for a merge with only one stream, we cannot distinguish whether it is a BITMAPEXECUTE model
    * or distributed table. The only thing we can do is assuming this stream can be used to execute expression.
    * This assumption is correct as follows.
    * 1. If it is a distributed table and it has only one stream, execute expression in advanced will not affect the correctness of the result.
    *    If it has more than one stream, we lost some performance to execute first stream's expression. The first stream will merge other streams again and set is_finished as false.
    *    Then caculate the result in the insertResultInto function.
    * 2. If it is a BITMAPEXECUTE model, each stream will be passed to merge function independently in ParallelBitMapInputStream so that each stream will set is_finished as true.
    *
    */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapMultiCountData<T> &>(this->data(rhs));

        /** uncomment for correctness. If the first stream comes here and WITHOUT a key
        * it will put an empty bitmap in the final_key. That will cause later insert
        * to final_key fail. The related logic is in executeExpressionOnlyOr()->bitmap.extract
        if (lhs_data.bitmap_data.empty())
        {
            mergeSingleStream(rhs_data);
            lhs_data.merge(rhs_data);
        }
        else
        */
        if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            for (size_t i = 0; i < final_keys.size(); i++)
                lhs_data.count_vector[i] += rhs_data.count_vector[i];
        }
            // If two stream are both false on is_finished, it must not a BITMAPEXECUTE model, so merge them and delay expression execution on insertResultInto
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
            // If one of stream's is_finished is false, it means
            // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
            // 2. it may be execution on distributed table
            // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapMultiCountData<T> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }
        Array res;
        for (auto count: local_data.count_vector)
            res.push_back(count);
        static_cast<ColumnArray &>(to).insert(res);
    }

};

template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapMultiCountWithDate final :
    public IAggregateFunctionDataHelper<AggregateFunctionBitMapMultiCountData<String>, AggregateFunctionBitMapMultiCountWithDate<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<String>>;
private:
    BitMapExpressionWithDateMultiAnalyzer analyzer;
    std::vector<String> final_keys;
    std::unordered_set<String> keys_without_date;
public:
    AggregateFunctionBitMapMultiCountWithDate(const DataTypes & argument_types_, const std::vector<String> & expression_)
            : IAggregateFunctionDataHelper<AggregateFunctionBitMapMultiCountData<String>, AggregateFunctionBitMapMultiCountWithDate<T>>(argument_types_, {}), analyzer(expression_)
    {
        final_keys = analyzer.final_keys;
        keys_without_date = analyzer.keys_without_date;
    }

    String getName() const override { return "BitmapMultiCountWithDate"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        String date;
        try
        {
            date = std::to_string(columns[0]->getInt(row_num));
        }
        catch(const Exception & e) {
            throw Exception("Exception when get data from 1st argument of BitmapMultiCountWithDate, message: " + e.message(), e.code());
        }

        {
            T origin_key = get_bitmap_key_from_column<T>(columns[1], row_num);

            if (check_is_internal_bitmap_key(origin_key))
                throw Exception(
                    fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", origin_key)
                    , ErrorCodes::BAD_ARGUMENTS
                );
        }

        String key = get_bitmap_string_key_from_column<T>(columns[1], row_num);

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        try {
            const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(*columns[2]);
            auto & bitmaps_data = this->data(place).bitmap_data;

            String date_with_key = date + "_" + key;
            const auto & bitmap = column_bitmap.getBitMapAt(row_num);
            if (analyzer.interested_tokens.count(date_with_key))
            {
                bitmaps_data.add(date_with_key, bitmap);
            }
            if (keys_without_date.count(key))
            {
                bitmaps_data.add(key, bitmap);
            }
        } catch (std::bad_cast &) {
            throw Exception("The third argument for BitmapMultiCountWithDate has to be Bitmap", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    bool mergeSingleStream(AggregateFunctionBitMapMultiCountData<String> & bitmap_count_data) const
    {
        auto & bitmap_data = bitmap_count_data.bitmap_data;
        auto & count_vector = bitmap_count_data.count_vector;

        if (bitmap_data.is_finished)
            return false;
        count_vector.clear();
        for (size_t i = 0; i < final_keys.size(); i++)
        {
            analyzer.executeExpression(bitmap_data, i);
            count_vector.push_back(bitmap_data.getCardinality(final_keys[i]));
        }
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;
        for (const auto& final_key: final_keys)
        {
            auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
            auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
            bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
            bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();
            if (lhs_bitmap_exists && rhs_bitmap_exists)
            {
                lhs_bitmap_it->second |= rhs_bitmap_it->second;
            }
            else
                throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    /*
    * The merge function is based on the fact different stream can caculate its result independently.
    * If we use BITMAPEXECUTE, ParallelBitMapBlockInputStream will produce many streams and invoke this merge
    * function for each stream. And only the ParallelBitMapBlockInputStream can invoke merge with only one stream, so
    * we can caculate expression for this single stream directly and set is_final as true.
    * When there are two streams, it may be invoked by ParallelBitMapBlockInputStream or normal aggregating stream. But,
    * if is_finished is true for both two streams, we can ensure these two streams can caculate directly.
    * Otherwise, handle them as normal aggregate functions, that is, use bitmap or to get all bitmap and caculate expression
    * in insertResultInto functions
    *
    * For distributed queries, we also have two merge model:
    * 1. distributed_perfect_shard: each node performs the merge as single node, the final result is merged by sum or bitmapOr function.
    * 2. distributed table: each node performs the aggregation at local and return a intermediate status, then at the coordinator node, it performs a
    * second aggregation. There exists the case the second aggregation has multiple merges - the first merge has single stream, and then merge the following
    * streams.
    *
    * The second case will obstruct the determine of BITMAPEXECUTE model, since for a merge with only one stream, we cannot distinguish whether it is a BITMAPEXECUTE model
    * or distributed table. The only thing we can do is assuming this stream can be used to execute expression.
    * This assumption is correct as follows.
    * 1. If it is a distributed table and it has only one stream, execute expression in advanced will not affect the correctness of the result.
    *    If it has more than one stream, we lost some performance to execute first stream's expression. The first stream will merge other streams again and set is_finished as false.
    *    Then caculate the result in the insertResultInto function.
    * 2. If it is a BITMAPEXECUTE model, each stream will be passed to merge function independently in ParallelBitMapInputStream so that each stream will set is_finished as true.
    *
    */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapMultiCountData<String> &>(this->data(rhs));

        /** uncomment for correctness. If the first stream comes here and WITHOUT a key
        * it will put an empty bitmap in the final_key. That will cause later insert
        * to final_key fail. The related logic is in executeExpressionOnlyOr()->bitmap.extract
        if (lhs_data.bitmap_data.empty())
        {
            mergeSingleStream(rhs_data);
            lhs_data.merge(rhs_data);
        }
        else
        */
        if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            for (size_t i = 0; i < final_keys.size(); i++)
                lhs_data.count_vector[i] += rhs_data.count_vector[i];
        }
            // If two stream are both false on is_finished, it must not a BITMAPEXECUTE model, so merge them and delay expression execution on insertResultInto
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
            // If one of stream's is_finished is false, it means
            // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
            // 2. it may be execution on distributed table
            // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapMultiCountData<String> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }
        Array res;
        for (auto count: local_data.count_vector)
            res.push_back(count);
        static_cast<ColumnArray &>(to).insert(res);
    }

};

template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapExtract final : public IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<T>, AggregateFunctionBitMapExtract<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
private:
    BitMapExpressionAnalyzer<T> analyzer;
    T final_key;
    UInt64 is_bitmap_execute = 0;
public:
    AggregateFunctionBitMapExtract(const DataTypes & argument_types_, String expression_, UInt64 is_bitmap_execute_)
    : IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<T>, AggregateFunctionBitMapExtract<T>>(argument_types_, {})
    , analyzer(expression_), is_bitmap_execute(is_bitmap_execute_)
    {
        final_key = analyzer.final_key;
    }

    String getName() const override { return "BitmapExtract"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeBitMap64>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        T key = get_bitmap_key_from_column<T>(columns[0], row_num);

        if (check_is_internal_bitmap_key(key))
            throw Exception(
                fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", key)
                , ErrorCodes::BAD_ARGUMENTS
            );

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        const auto & column_bitmap = static_cast<const ColumnBitMap64 &>(*columns[1]);

        const BitMap64 & bitmap = column_bitmap.getBitMapAt(row_num);

        auto & bitmap_data = this->data(place).bitmap_data;

        bitmap_data.add(key, bitmap);
    }

    bool mergeSingleStream(AggregateFunctionBitMapExtractData<T> & bitmap_extract_data) const
    {
        auto & bitmap_data = bitmap_extract_data.bitmap_data;

        if (bitmap_data.is_finished)
            return false;

        analyzer.executeExpression(bitmap_data);
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;

        auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
        auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
        bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
        bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();

        if (lhs_bitmap_exists && rhs_bitmap_exists)
        {
            lhs_bitmap_it->second |= rhs_bitmap_it->second;
        }
        else
            throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::LOGICAL_ERROR);
    }

    /*
    * The merge function is based on the fact different stream can caculate its result independently.
    * If we use BITMAPEXECUTE, ParallelBitMapBlockInputStream will produce many streams and invoke this merge
    * function for each stream. And only the ParallelBitMapBlockInputStream can invoke merge with only one stream, so
    * we can caculate expression for this single stream directly and set is_final as true.
    * When there are two streams, it may be invoked by ParallelBitMapBlockInputStream or normal aggregating stream. But,
    * if is_finished is true for both two streams, we can ensure these two streams can be merged by mergeTwoStreams.
    * Otherwise, handle them as normal aggregate functions.
    *
    * For distributed queries, we also have two merge model:
    * 1. distributed_perfect_shard: each node performs the merge as single node, the final result is merged by sum or bitmapOr function.
    * 2. distributed table: each node performs the aggregation at local and return a intermediate status, then at the coordinator node, it performs a
    * second aggregation. There exists the case the second aggregation has multiple merges - the first merge has single stream, and then merge the following
    * streams.
    *
    * The second case will obstruct the determine of BITMAPEXECUTE model, since for a merge with only one stream, we cannot distinguish whether it is a BITMAPEXECUTE model
    * or distributed table. The only thing we can do is assuming this stream can be used to execute expression.
    * This assumption is correct as follows.
    * 1. If it is a distributed table and it has only one stream, execute expression in advanced will not affect the correctness of the result.
    *    If it has more than one stream, we lost some performance to execute first stream's expression. The first stream will merge other streams again and set is_finished as false.
    *    Then caculate the result in the insertResultInto function.
    * 2. If it is a BITMAPEXECUTE model, each stream will be passed to merge function independently in ParallelBitMapInputStream so that each stream will set is_finished as true.
    *
    */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapExtractData<T> &>(this->data(rhs));

        if (is_bitmap_execute && lhs_data.bitmap_data.empty())
        {
            mergeSingleStream(rhs_data);
            lhs_data.merge(rhs_data);
        }
        else if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            mergeTwoStreams(place, rhs);
        }
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
        // If one of stream's is_finished is false, it means
        // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
        // 2. it may be execution on distributed table
        // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapExtractData<T> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }

        auto it = local_data.bitmap_data.bitmap_map.find(final_key);
        BitMap64 bitmap;
        if (it != local_data.bitmap_data.bitmap_map.end())
            bitmap = it->second;

        static_cast<ColumnBitMap64 &>(to).insert(std::move(bitmap));
    }

};

template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapMultiExtract final :
    public IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<T>, AggregateFunctionBitMapMultiExtract<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<T>>;
private:
    BitMapExpressionMultiAnalyzer<T> analyzer;
    std::vector<T> final_keys;
public:
    AggregateFunctionBitMapMultiExtract(const DataTypes & argument_types_, const std::vector<String>& expression_)
    : IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<T>, AggregateFunctionBitMapMultiExtract<T>>(argument_types_, {})
    , analyzer(expression_)
    {
        final_keys = analyzer.final_keys;
    }

    String getName() const override { return "BitmapMultiExtract"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeBitMap64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        T key = get_bitmap_key_from_column<T>(columns[0], row_num);

        if (check_is_internal_bitmap_key(key))
            throw Exception(
                fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", key)
                , ErrorCodes::BAD_ARGUMENTS
            );

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        const auto & column_bitmap = static_cast<const ColumnBitMap64 &>(*columns[1]);

        const BitMap64 & bitmap = column_bitmap.getBitMapAt(row_num);

        auto & bitmap_data = this->data(place).bitmap_data;

        bitmap_data.add(key, bitmap);
    }

    bool mergeSingleStream(AggregateFunctionBitMapExtractData<T> & bitmap_extract_data) const
    {
        auto & bitmap_data = bitmap_extract_data.bitmap_data;

        if (bitmap_data.is_finished)
            return false;

        for (size_t i = 0; i < final_keys.size(); i++)
        {
            analyzer.executeExpression(bitmap_data, i);
        }
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;

        for (auto& final_key: final_keys)
        {
            auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
            auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
            bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
            bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();

            if (lhs_bitmap_exists && rhs_bitmap_exists)
            {
                lhs_bitmap_it->second |= rhs_bitmap_it->second;
            }
            else
                throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapExtractData<T> &>(this->data(rhs));

        /// uncomment for correctness. If the first stream comes here and WITHOUT a key
        /// it will put an empty bitmap in the final_key. That will cause later insert
        /// to final_key fail. The related logic is in executeExpressionOnlyOr()->bitmap.extract
        // if (lhs_data.bitmap_data.empty())
        // {
        //     mergeSingleStream(rhs_data);
        //     lhs_data.merge(rhs_data);
        // }
        // else
        if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            mergeTwoStreams(place, rhs);
        }
            // If two stream are both false on is_finished, it must not a BITMAPEXECUTE model, so merge them and delay expression execution on insertResultInto
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
            // If one of stream's is_finished is false, it means
            // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
            // 2. it may be execution on distributed table
            // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapExtractData<T> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }

        Array res;
        for (auto& final_key: final_keys)
        {
            auto it = local_data.bitmap_data.bitmap_map.find(final_key);
            if (it != local_data.bitmap_data.bitmap_map.end())
            {
                res.push_back(it->second);
            }
        }

        static_cast<ColumnArray &>(to).insert(res);
    }

};

template<typename T, typename = std::enable_if_t< std::is_integral_v<T> || std::is_same_v<T, String> > >
class AggregateFunctionBitMapMultiExtractWithDate final :
    public IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<String>, AggregateFunctionBitMapMultiExtractWithDate<T>>
{
    using BitMapExpressions = std::vector<BitMapExpressionNode<String>>;
private:
    BitMapExpressionWithDateMultiAnalyzer analyzer;
    std::vector<String> final_keys;
    std::unordered_set<String> keys_without_date;
public:
    AggregateFunctionBitMapMultiExtractWithDate(const DataTypes & argument_types_, std::vector<String> expression_)
            : IAggregateFunctionDataHelper<AggregateFunctionBitMapExtractData<String>, AggregateFunctionBitMapMultiExtractWithDate<T>>(argument_types_, {}), analyzer(expression_)
    {
        final_keys = analyzer.final_keys;
        keys_without_date = analyzer.keys_without_date;
    }

    String getName() const override { return "BitmapMultiExtractWithDate"; }
    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeBitMap64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        String date;
        try
        {
            date = std::to_string(columns[0]->getInt(row_num));
        }
        catch(const Exception & e) {
            throw Exception("Exception when get data from 1st argument of BitmapMultiCountWithDate, message: " + e.message(), e.code());
        }

        {
            T origin_key = get_bitmap_key_from_column<T>(columns[1], row_num);

            if (check_is_internal_bitmap_key(origin_key))
                throw Exception(
                    fmt::format("The tag (or bitmap key): {} affects the computation, please change another number, maybe positive number is better", origin_key)
                    , ErrorCodes::BAD_ARGUMENTS
                );
        }

        String key = get_bitmap_string_key_from_column<T>(columns[1], row_num);

        if constexpr (std::is_same_v<T, String>)
        {
            if (existBitengineExpressionKeyword(key))
                throw Exception("The tag (or bitmap key): " + key + " has illegal character, " +
                    "please check your tag whether contains following characters ['&' , '|' , '~' , ',' , '#' , ' ' , '(' , ')']", ErrorCodes::BAD_ARGUMENTS);
        }

        try {
            const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(*columns[2]);
            auto & bitmaps_data = this->data(place).bitmap_data;

            String date_with_key = date + "_" + key;
            const auto & bitmap = column_bitmap.getBitMapAt(row_num);
            if (analyzer.interested_tokens.count(date_with_key))
            {
                bitmaps_data.add(date_with_key, bitmap);
            }
            if (keys_without_date.count(key))
            {
                bitmaps_data.add(key, bitmap);
            }
        } catch (std::bad_cast &) {
            throw Exception("The third argument for BitmapMultiCountWithDate has to be Bitmap", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    bool mergeSingleStream(AggregateFunctionBitMapExtractData<String> & bitmap_extract_data) const
    {
        auto & bitmap_data = bitmap_extract_data.bitmap_data;

        if (bitmap_data.is_finished)
            return false;
        for (size_t i = 0; i < final_keys.size(); i++)
        {
            analyzer.executeExpression(bitmap_data, i);
        }
        bitmap_data.is_finished = true;

        return true;
    }

    void mergeTwoStreams(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs) const
    {
        auto & lhs_bitmap_data = this->data(place).bitmap_data;
        const auto & rhs_bitmap_data = this->data(rhs).bitmap_data;
        for (const auto& final_key: final_keys)
        {
            auto lhs_bitmap_it = lhs_bitmap_data.bitmap_map.find(final_key);
            auto rhs_bitmap_it = rhs_bitmap_data.bitmap_map.find(final_key);
            bool lhs_bitmap_exists = lhs_bitmap_it != lhs_bitmap_data.bitmap_map.end();
            bool rhs_bitmap_exists = rhs_bitmap_it != rhs_bitmap_data.bitmap_map.end();
            if (lhs_bitmap_exists && rhs_bitmap_exists)
            {
                lhs_bitmap_it->second |= rhs_bitmap_it->second;
            }
            else
                throw Exception("Cannot find final key when merge two streams in " + getName(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    /*
    * The merge function is based on the fact different stream can caculate its result independently.
    * If we use BITMAPEXECUTE, ParallelBitMapBlockInputStream will produce many streams and invoke this merge
    * function for each stream. And only the ParallelBitMapBlockInputStream can invoke merge with only one stream, so
    * we can caculate expression for this single stream directly and set is_final as true.
    * When there are two streams, it may be invoked by ParallelBitMapBlockInputStream or normal aggregating stream. But,
    * if is_finished is true for both two streams, we can ensure these two streams can caculate directly.
    * Otherwise, handle them as normal aggregate functions, that is, use bitmap or to get all bitmap and caculate expression
    * in insertResultInto functions
    *
    * For distributed queries, we also have two merge model:
    * 1. distributed_perfect_shard: each node performs the merge as single node, the final result is merged by sum or bitmapOr function.
    * 2. distributed table: each node performs the aggregation at local and return a intermediate status, then at the coordinator node, it performs a
    * second aggregation. There exists the case the second aggregation has multiple merges - the first merge has single stream, and then merge the following
    * streams.
    *
    * The second case will obstruct the determine of BITMAPEXECUTE model, since for a merge with only one stream, we cannot distinguish whether it is a BITMAPEXECUTE model
    * or distributed table. The only thing we can do is assuming this stream can be used to execute expression.
    * This assumption is correct as follows.
    * 1. If it is a distributed table and it has only one stream, execute expression in advanced will not affect the correctness of the result.
    *    If it has more than one stream, we lost some performance to execute first stream's expression. The first stream will merge other streams again and set is_finished as false.
    *    Then caculate the result in the insertResultInto function.
    * 2. If it is a BITMAPEXECUTE model, each stream will be passed to merge function independently in ParallelBitMapInputStream so that each stream will set is_finished as true.
    *
    */
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = const_cast<AggregateFunctionBitMapExtractData<String> &>(this->data(rhs));

        /// uncomment for correctness. If the first stream comes here and WITHOUT a key
        /// it will put an empty bitmap in the final_key. That will cause later insert
        /// to final_key fail. The related logic is in executeExpressionOnlyOr()->bitmap.extract
        // if (lhs_data.bitmap_data.empty())
        // {
        //     mergeSingleStream(rhs_data);
        //     lhs_data.merge(rhs_data);
        // }
        // else
        if (lhs_data.bitmap_data.is_finished && rhs_data.bitmap_data.is_finished)
        {
            mergeTwoStreams(place, rhs);
        }
            // If two stream are both false on is_finished, it must not a BITMAPEXECUTE model, so merge them and delay expression execution on insertResultInto
        else if (!lhs_data.bitmap_data.is_finished && !rhs_data.bitmap_data.is_finished)
        {
            lhs_data.merge(rhs_data);
        }
            // If one of stream's is_finished is false, it means
            // 1. it is a BITMAPEXECUTE model but not a distributed_perfect_shard model.
            // 2. it may be execution on distributed table
            // So it is better to merge them to guarantee the correctness of result
        else
        {
            lhs_data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    // If is_finished is false, aggregate functions need to caculate expression.
    // It means the aggregate function is a normal aggregate (not a BITMAPEXECUTE), so the final
    // result is computed in insertResultInto
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & local_data = const_cast<AggregateFunctionBitMapExtractData<String> &>(this->data(place));

        if (!local_data.bitmap_data.is_finished)
        {
            mergeSingleStream(local_data);
        }
        Array res;
        for (const auto& final_key: final_keys)
        {
            auto it = local_data.bitmap_data.bitmap_map.find(final_key);
            if (it != local_data.bitmap_data.bitmap_map.end())
            {
                res.push_back(it->second);
            }
        }
        static_cast<ColumnArray &>(to).insert(res);
    }

};

}
