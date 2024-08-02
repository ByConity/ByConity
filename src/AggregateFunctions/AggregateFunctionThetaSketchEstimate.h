//
// Created by vita.lai on 2022/7/9.
//
#pragma once

#include <DataSketches/theta_union.hpp>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeSketchBinary.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/SketchAllocator.h>
#include <Columns/ColumnSketchBinary.h>


namespace DB
{
template <UInt8 K>
struct AggregateFunctionThetaSketchEstimateData
{
    datasketches::theta_union sk_union;
    AggregateFunctionThetaSketchEstimateData():sk_union(datasketches::theta_union::builder().set_lg_k(K).build()){}
    static String getName() { return "theta_sketch"; }
};

template <typename T, UInt8 K>
class AggregateFunctionThetaSketchEstimate final
    : public IAggregateFunctionDataHelper<AggregateFunctionThetaSketchEstimateData<K>, AggregateFunctionThetaSketchEstimate<T, K>>
{
public:
    AggregateFunctionThetaSketchEstimate(const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionThetaSketchEstimateData<K>, AggregateFunctionThetaSketchEstimate>(argument_types_, params_), ignore_wrong_data(ignore_wrong_data_) {}

    String getName() const override
    {
        return "thetaSketchEstimate";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        try
        {
            // String is for new datatype "Sketch"
            if constexpr (std::is_same_v<T, DataTypeSketchBinary>)
            {
                const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
                if (ignore_wrong_data && value.size == 0)
                    return;
                // datasketches::compact_theta_sketch thetaSketch = datasketches::compact_theta_sketch::deserialize(value.data, value.size, datasketches::DEFAULT_SEED, AggregateFunctionThetaSketchAllocator());
                this->data(place).sk_union.update(datasketches::wrapped_compact_theta_sketch::wrap(value.data, value.size));
            }
            else if constexpr (std::is_same_v<T, DataTypeAggregateFunction>)
            {
                //the format of this value should be the same with serialize
                const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
                if (ignore_wrong_data && value.size == 0)
                    return;
                // ReadBuffer buf(const_cast<char *>(value.data), value.size);
                // this->data(place).sk_union.update(readThetaSketch(buf));
                this->data(place).sk_union.update(datasketches::wrapped_compact_theta_sketch::wrap(value.data, value.size));
            }
            else
            {
                StringRef value = columns[0]->getDataAt(row_num);
                datasketches::update_theta_sketch sk_update = datasketches::update_theta_sketch::builder().build();
                sk_update.update(value.toString());
                this->data(place).sk_union.update(sk_update);
            }
        }
        catch (std::exception & e)
        {
            if (!ignore_wrong_data)
                throw e;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
       this->data(place).sk_union.update(this->data(rhs).sk_union.get_result());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).sk_union.get_result().serialize(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).sk_union.update(readThetaSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        static_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).sk_union.get_result().get_estimate());
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    inline datasketches::compact_theta_sketch readThetaSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::compact_theta_sketch::deserialize(d.data(), d.size(), datasketches::DEFAULT_SEED, AggregateFunctionThetaSketchAllocator());
    }
};
}
