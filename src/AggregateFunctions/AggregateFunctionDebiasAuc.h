#pragma once

//
// Created by zhengxudong.alpha@bytedance.com on 2019/4/14.
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

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

struct FunctionDebiasAucData
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<std::pair<Float64, Float64>, 4096, ArenaAllocator>;
    Array stat_array;
};

template<typename P, typename L, typename S>
class FunctionDebiasAuc: public IAggregateFunctionDataHelper<FunctionDebiasAucData, FunctionDebiasAuc<P, L, S>>
{
private:
    Float64 precision;
    Float64 min;
    Float64 max;
    UInt64 bucket_num;

    UInt64 getBucket(const Float64 pred) const
    {
        return std::min(std::max(static_cast<const UInt64>(((pred - min) / precision)), UInt64(0)), bucket_num - 1);
    }

    Float64 calcDebiasAuc(ConstAggregateDataPtr __restrict place) const
    {
        auto & stat_array = this->data(place).stat_array;
        // UInt64 total_rank = 0;
        // UInt64 sum_pos_rank = 0;
        Float64 sum_pos_rate = 0;
        // UInt64 num_pos = 0;
        Float64 pos_rate_counter = 0;
        // UInt64 num_neg = 0;
        Float64 neg_rate_counter = 0;
        for (size_t i = 0; i < stat_array.size(); ++i)
        {
            Float64 pos = stat_array[i].first;
            Float64 neg = stat_array[i].second;
            // num_pos += pos;
            pos_rate_counter += pos;
            // num_neg += neg;
            neg_rate_counter += neg;
            // auto avg_rank = total_rank + (pos + neg + 1) / 2.0;
            auto avg_neg_rate = neg_rate_counter - neg / 2.0;
            // sum_pos_rank += avg_rank * pos;
            sum_pos_rate += avg_neg_rate * pos;
            // total_rank += (pos + neg);
        }

        Float64 result = 0.0;
        // if (num_neg == 0 || num_pos == 0)
        if (pos_rate_counter == 0 || neg_rate_counter == 0)
            result = 1.0;
        else
            // result = (sum_pos_rank - num_pos * (num_pos + 1) / 2) / Float64(num_pos) / Float64(num_neg);
            result = sum_pos_rate / Float64(pos_rate_counter) / Float64(neg_rate_counter);
        return result;
    }

public:
    FunctionDebiasAuc(const DataTypes & argument_types_, const Array & params_,
            Float64 precision_ = 0.00001, Float64 min_ = 0.0, Float64 max_ = 1.0)
        : IAggregateFunctionDataHelper<FunctionDebiasAucData, FunctionDebiasAuc<P, L, S>>(argument_types_, params_)
          , precision(precision_), min(min_), max(max_)
    {
        bucket_num = static_cast<const UInt64>(std::ceil((max - min) / precision));
    }

    String getName() const override
    { return "debiasAuc"; }

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & stat_array = this->data(place).stat_array;

        if (stat_array.size() != bucket_num) {
            stat_array.resize_fill(bucket_num, std::make_pair<Float64, Float64>(0, 0), arena);
        }

        const auto& pred = static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num];
        const auto& label = static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num];
        const auto& sample_rate = static_cast<const ColumnVector<S> &>(*columns[2]).getData()[row_num];
        if (sample_rate == 0)
            throw Exception("Parameter sample_rate contain 0", ErrorCodes::BAD_ARGUMENTS);

        UInt64 bucket = this->getBucket(static_cast<const Float64>(pred));

        if (label > 0) {
            // stat_array[bucket].first++;
            stat_array[bucket].first = stat_array[bucket].first + 1.0/sample_rate;
        } else {
            // stat_array[bucket].second++;
            stat_array[bucket].second = stat_array[bucket].second + 1.0/sample_rate;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & lhs_stat = this->data(place).stat_array;
        auto & rhs_stat = this->data(rhs).stat_array;
        if (lhs_stat.size() != bucket_num) {
            lhs_stat.resize_fill(bucket_num, std::make_pair<Float64, Float64>(0, 0), arena);
        }
        for (size_t i = 0; i < std::min(UInt64(rhs_stat.size()), bucket_num); i++) {
            lhs_stat[i].first += rhs_stat[i].first;
            lhs_stat[i].second += rhs_stat[i].second;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        const auto & stat_array = this->data(place).stat_array;
        size_t size = stat_array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&stat_array[0]), size * sizeof(stat_array[0]));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        auto & stat_array = this->data(place).stat_array;
        stat_array.resize(bucket_num, arena);
        buf.read(reinterpret_cast<char *>(&stat_array[0]), size * sizeof(stat_array[0]));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = calcDebiasAuc(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }
};

}
