#pragma once

#include <random>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>

#include <Common/PODArray.h>
#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{

template<typename P, typename L>
struct FunctionFastAucData
{
    UInt64 sum_pos = 0;
    UInt64 sum_neg = 0;
    P pos = -1;
    P neg = -1;
    P last_pred = -1;
    L last_label = -1;
    UInt64 count = 0;
};

template<typename P, typename L>
class FunctionFastAuc: public IAggregateFunctionDataHelper<FunctionFastAucData<P, L>, FunctionFastAuc<P, L>>
{
private:
    bool is_regression;

    Float64 calcFastAuc(ConstAggregateDataPtr __restrict place) const
    {
        if (this->data(place).sum_pos + this->data(place).sum_neg == 0)
            return 1.0;
        else
            return static_cast<Float64>(this->data(place).sum_pos) / static_cast<Float64>(this->data(place).sum_pos + this->data(place).sum_neg);
    }

public:
    FunctionFastAuc(const DataTypes & argument_types_, const bool is_regression_ = false)
        : IAggregateFunctionDataHelper<FunctionFastAucData<P, L>, FunctionFastAuc<P, L>>(argument_types_, {}),
          is_regression(is_regression_)
        {}

    String getName() const override
    { return "fastAuc"; }

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto& pred = static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num];
        const auto& label = static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num];
        auto& data = this->data(place);
        if (!is_regression) {
            if (label > 1e-6) {
                if (data.neg >= 0) {
                    if (pred > data.neg) {
                        data.sum_pos += 1;
                    } else {
                        data.sum_neg += 1;
                    }
                }
                data.pos = pred;
            } else {
                if (data.pos >= 0) {
                    if (pred < data.pos) {
                        data.sum_pos += 1;
                    } else {
                        data.sum_neg += 1;
                    }
                }
                data.neg = pred;
            }
        } else {
            if (data.count > 0) {
                if (pred >= data.last_pred && label >= data.last_label) {
                    data.sum_pos += 1;
                } else {
                    data.sum_neg += 1;
                }
            }
            data.last_pred = pred;
            data.last_label = label;
            data.count++;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = this->data(rhs);
        lhs_data.sum_pos += rhs_data.sum_pos;
        lhs_data.sum_neg += rhs_data.sum_neg;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum_pos, buf);
        writeBinary(this->data(place).sum_neg, buf);
        writeBinary(this->data(place).pos, buf);
        writeBinary(this->data(place).neg, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum_pos, buf);
        readBinary(this->data(place).sum_neg, buf);
        readBinary(this->data(place).pos, buf);
        readBinary(this->data(place).neg, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = calcFastAuc(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }
};

}
