#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Common/ArenaAllocator.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <cmath>

namespace DB
{

struct FunctionFastAuc2Data
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<std::pair<UInt64, UInt64>, 4096, ArenaAllocator>;
    Array stat_array;
};

template<typename P, typename L>
class FunctionFastAuc2: public IAggregateFunctionDataHelper<FunctionFastAuc2Data, FunctionFastAuc2<P, L>>
{
private:
    Float64 precision;
    Float64 min;
    Float64 max;
    UInt64 bucket_num;

    UInt64 getBucket(const Float64 pred) const
    {
        return std::min(std::max(static_cast<UInt64>((pred - min) / precision), 0ul), bucket_num - 1);
    }

    Float64 calcFastAuc2(ConstAggregateDataPtr place) const
    {
        const auto & stat_array = this->data(place).stat_array;
        UInt64 total_rank = 0;
        UInt64 sum_pos_rank = 0;
        UInt64 num_pos = 0;
        UInt64 num_neg = 0;
        for (size_t i = 0; i < stat_array.size(); ++i)
        {
            UInt64 pos = stat_array[i].first;
            UInt64 neg = stat_array[i].second;
            num_pos += pos;
            num_neg += neg;
            auto avg_rank = total_rank + (pos + neg + 1) / 2.0;
            sum_pos_rank += avg_rank * pos;
            total_rank += (pos + neg);
        }

        if (!num_neg || !num_pos)
            return 1.0;

        return 1.0 * (sum_pos_rank - 1.0 * num_pos * (num_pos + 1) / 2) / (num_pos * num_neg);
    }

public:
    FunctionFastAuc2(const DataTypes & arguments, const Array & params, Float64 precision_ = 0.00001, Float64 min_ = 0.0, Float64 max_ = 1.0)
        : IAggregateFunctionDataHelper<FunctionFastAuc2Data, FunctionFastAuc2<P, L>>(arguments, params)
        , precision(precision_), min(min_), max(max_)
        , bucket_num(static_cast<UInt64>(std::ceil((max - min) / precision)))
    {
    }

    String getName() const override
    {
        return "fastAuc2";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & stat_array = this->data(place).stat_array;

        if (stat_array.size() != bucket_num)
            stat_array.resize_fill(bucket_num, {0, 0}, arena);

        const auto & pred = static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num];
        const auto & label = static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num];

        UInt64 bucket = this->getBucket(pred);

        if (label > 0)
            stat_array[bucket].first++;
        else
            stat_array[bucket].second++;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & lhs_stat = this->data(place).stat_array;
        auto & rhs_stat = this->data(rhs).stat_array;
        if (lhs_stat.size() != bucket_num)
            lhs_stat.resize_fill(bucket_num, {0, 0}, arena);

        for (size_t i = 0; i < std::min(rhs_stat.size(), bucket_num); i++)
        {
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
        Float64 result = calcFastAuc2(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }

    bool allocatesMemoryInArena() const override { return false; }
};

}
