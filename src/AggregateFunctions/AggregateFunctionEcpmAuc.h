#pragma once

//
// Created by hongyi.zhang@bytedance.com on 2020/1/8.
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

struct FunctionEcpmAucData
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<std::pair<Float64, Float64>, 4096, ArenaAllocator>;
    Array stat_array;
};

template<typename P, typename L>
class FunctionEcpmAuc: public IAggregateFunctionDataHelper<FunctionEcpmAucData, FunctionEcpmAuc<P, L>>
{
private:
    Float64 precision;
    Float64 min;
    Float64 max;
    Float64 interval;
    UInt64 bucket_num;

    UInt64 getBucket(const Float64 ecpm) const
    {
        return std::min(std::max(static_cast<const UInt64>((max - log10(ecpm)) / (precision * interval)), UInt64(0)), bucket_num - 1);
    }

    Float64 calcEcpmAuc(ConstAggregateDataPtr __restrict place) const
    {
        auto & stat_array = this->data(place).stat_array;
        Float64 total_ecpm = 0.0;
        Float64 total_adv_value = 0.0;
        Float64 partial_auc_lb = 0.0;
        Float64 partial_auc_ub = 0.0;
        
        for (size_t i = 0; i < stat_array.size(); ++i)
        {
            partial_auc_lb += stat_array[i].first * total_adv_value;
            total_ecpm += stat_array[i].first;
            total_adv_value += stat_array[i].second;
            partial_auc_ub += stat_array[i].first * total_adv_value;
        }

        Float64 result = (partial_auc_lb + partial_auc_ub) / (total_ecpm * total_adv_value * 2.0);
        return result;
    }

public:
    FunctionEcpmAuc(const DataTypes & argument_types_, const Array & params_,
            Float64 precision_ = 0.00001, Float64 min_ = -2.5, Float64 max_ = 2.5)
        : IAggregateFunctionDataHelper<FunctionEcpmAucData, FunctionEcpmAuc<P, L>>(argument_types_, params_)
          , precision(precision_), min(min_), max(max_)
    {
        interval = max - min;
        bucket_num = static_cast<const UInt64>(std::ceil(1.0 / precision));
    }

    String getName() const override
    { return "ecpmAuc"; }

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & stat_array = this->data(place).stat_array;

        if (stat_array.size() != bucket_num) {
            stat_array.resize_fill(bucket_num, std::make_pair<Float64, Float64>(0, 0), arena);
        }

        const auto& ecpm = static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num];
        const auto& adv_value = static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num];

        UInt64 bucket = this->getBucket(static_cast<const Float64>(ecpm));

        stat_array[bucket].first++;
        stat_array[bucket].second += static_cast<const Float64>(adv_value);
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
        Float64 result = calcEcpmAuc(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }
};

}
