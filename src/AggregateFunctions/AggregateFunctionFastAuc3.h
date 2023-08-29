#pragma once

//
// Created by zouyang.233@bytedance.com on 2022/11/22.
//

#include <math.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>

#include <Common/PODArray.h>
#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <string>
#include <iostream>

namespace DB
{

struct FunctionFastAuc3Data
{
    using Allocator = MixedArenaAllocator<4096>;
    using Map = std::unordered_map<int, UInt64>;
    Map pos_map;
    Map neg_map;
};

template<typename P, typename L>
class FunctionFastAuc3: public IAggregateFunctionDataHelper<FunctionFastAuc3Data, FunctionFastAuc3<P, L>>
{
private:
    Float64 precision;
    Float64 min;
    Float64 max;
    UInt64 bucket_num;

    UInt64 getBucket(const Float64 pred) const
    {
        return std::min(std::max(static_cast<UInt64>((pred - min) / precision), UInt64(0)), bucket_num - 1);
    }

    Float64 calcFastAuc3(ConstAggregateDataPtr __restrict place) const
    {
        auto & pos_map = this->data(place).pos_map;
        auto & neg_map = this->data(place).neg_map;
        // std::cout << "\n[calcFastAuc3] pos map size is " << pos_map.size() << std::endl;;
        // std::cout << "\n[calcFastAuc3] neg map size is " << neg_map.size() << std::endl;;
        UInt64 num_pos = 0;
        UInt64 num_neg = 0;
        UInt64 total_rank = 0;
        Float64 sum_pos_rank = 0;
        for (size_t i = 0; i < bucket_num; ++i)
        {
            UInt64 pos = 0;
            UInt64 neg = 0;
            if (pos_map.find(i) != pos_map.end())
            {
                pos = pos_map.find(i)->second;
            }
            num_pos += pos;
            if (neg_map.find(i) != neg_map.end())
            {
                neg = neg_map.find(i)->second;
            }
            num_neg += neg;
            auto avg_rank = total_rank + (pos + neg + 1) / 2.0;
            sum_pos_rank += avg_rank * pos;
            total_rank += (pos + neg);
        }
        // std::cout << "\n[calcFastAuc3] num_neg is " << num_neg << " num_pos is " << num_pos << " sum_pos_rank is " << sum_pos_rank  << std::endl;;
        Float64 result = 0.0;
        if (num_neg == 0 || num_pos == 0)
            result = 1.0;
        else
            result = (sum_pos_rank - num_pos * (num_pos + 1) / 2.0) / Float64(num_neg) / Float64(num_pos);
        return result;
    }

public:
    FunctionFastAuc3(const DataTypes & argument_types_, const Array & params_,
            Float64 precision_ = 0.00001, Float64 min_ = 0.0, Float64 max_ = 1.0)
        : IAggregateFunctionDataHelper<FunctionFastAuc3Data, FunctionFastAuc3<P, L>>(argument_types_, params_)
          , precision(precision_), min(min_), max(max_)
    {
        bucket_num = static_cast<UInt64>(std::ceil((max - min) / precision));
    }

    String getName() const override
    { return "fastAuc3"; }

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, [[maybe_unused]] Arena * arena) const override
    {
        auto & pos_map = this->data(place).pos_map;
        auto & neg_map = this->data(place).neg_map;

        const auto& pred = static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num];
        const auto& label = static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num];

        UInt64 bucket = this->getBucket(static_cast<Float64>(pred));
        // std::cout << "\n[add] pred is " << pred << " bucket is " << bucket << std::endl;

        if (label > 0) {
            if (pos_map.find(bucket) == pos_map.end()) {
                pos_map.emplace(bucket, 1);
            } else {
                ++pos_map.find(bucket)->second;
            }
        } else {
            if (neg_map.find(bucket) == neg_map.end()) {
                neg_map.emplace(bucket, 1);
            } else {
                ++neg_map.find(bucket)->second;
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, [[maybe_unused]] Arena * arena) const override
    {
        auto & lhs_pos_map = this->data(place).pos_map;
        auto & lhs_neg_map = this->data(place).neg_map;
        auto & rhs_pos_map = this->data(rhs).pos_map;
        auto & rhs_neg_map = this->data(rhs).neg_map;
        
        for (auto x: rhs_pos_map) {
            if (lhs_pos_map.find(x.first) == lhs_pos_map.end()) {
                lhs_pos_map.emplace(x.first, x.second);
            } else {
                lhs_pos_map.find(x.first)->second += x.second;
            }
        }

        for (auto x: rhs_neg_map) {
            if (lhs_neg_map.find(x.first) == lhs_neg_map.end()) {
                lhs_neg_map.emplace(x.first, x.second);
            } else {
                lhs_neg_map.find(x.first)->second += x.second;
            }
        }

    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        const auto & pos_map = this->data(place).pos_map;
        const auto & neg_map = this->data(place).neg_map;
        writeVarUInt(pos_map.size(), buf);
        for (auto&& item : pos_map) {
          writeVarInt(item.first, buf);
          writeVarUInt(item.second, buf);
        }
        writeVarUInt(neg_map.size(), buf);
        for (auto&& item : neg_map) {
          writeVarInt(item.first, buf);
          writeVarUInt(item.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, [[maybe_unused]] Arena * arena) const override
    {
        auto & pos_map = this->data(place).pos_map;
        auto & neg_map = this->data(place).neg_map;
        size_t size = 0;
        readVarUInt(size, buf);
        pos_map.clear();
        for (size_t i = 0; i < size; i++) {
          Int64 idx;
          UInt64 val;
          readVarInt(idx, buf);
          readVarUInt(val, buf);
          pos_map[static_cast<int>(idx)] = val;
        }
        readVarUInt(size, buf);
        neg_map.clear();
        for (size_t i = 0; i < size; i++) {
          Int64 idx;
          UInt64 val;
          readVarInt(idx, buf);
          readVarUInt(val, buf);
          neg_map[static_cast<int>(idx)] = val;
        }
        // std::cout << "\n [deserialize] neg_map size is " << neg_map.size() << " pos_map size is " << pos_map.size() << std::endl;;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = calcFastAuc3(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }

};

}
