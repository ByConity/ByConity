#pragma once


#include <DataSketches/hll.hpp>
#include <DataSketches/kll_sketch.hpp>
#include <DataSketches/quantiles_sketch.hpp>

#include <Columns/ColumnSketchBinary.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeSketchBinary.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/SketchAllocator.h>

namespace DB
{
template <UInt8 K>
struct AggregateFunctionHllSketchEstimateData
{
    AggregateFunctionHllSketchEstimateData()
        : u(K) {}

    datasketches::hll_union u;

    static String getName() { return "hll_sketch"; }
};



template <UInt8 K>
class AggregateFunctionHLLSketchUnion final
    : public IAggregateFunctionDataHelper<AggregateFunctionHllSketchEstimateData<K>, AggregateFunctionHLLSketchUnion<K>>
{
public:
    AggregateFunctionHLLSketchUnion(const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionHllSketchEstimateData<K>, AggregateFunctionHLLSketchUnion>(argument_types_, params_), ignore_wrong_data(ignore_wrong_data_) {}

    String getName() const override
    {
        return "hllSketchUnion";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeSketchBinary>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        try
        {
            const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
            if (ignore_wrong_data && value.size == 0)
                return;
            datasketches::hll_sketch hll_sketch_data = datasketches::hll_sketch::deserialize(value.data, value.size, AggregateFunctionHllSketchAllocator());
            this->data(place).u.update(hll_sketch_data);
        }
        catch (std::exception & e)
        {
            if (!ignore_wrong_data)
                throw e;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).u.update(this->data(rhs).u.get_result());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).u.get_result().serialize_compact(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).u.update(readHLLSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        std::ostringstream oss;
        this->data(place).u.get_result().serialize_compact(oss);
        static_cast<ColumnSketchBinary &>(to).insert(Field(oss.str().data(), oss.str().size()));
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    inline datasketches::hll_sketch readHLLSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::hll_sketch::deserialize(d.data(), d.size(), AggregateFunctionHllSketchAllocator());
    }
};

template <typename T, UInt8 K>
class AggregateFunctionHllSketchEstimate final
   : public IAggregateFunctionDataHelper<AggregateFunctionHllSketchEstimateData<K>, AggregateFunctionHllSketchEstimate<T, K>>
{
public:
    AggregateFunctionHllSketchEstimate(const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionHllSketchEstimateData<K>, AggregateFunctionHllSketchEstimate>(argument_types_, params_), ignore_wrong_data(ignore_wrong_data_)
    {
        if (params_.size() == 2)
            use_composite_estimate = true;
    }

    String getName() const override
    {
        return "hllSketchEstimate";
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
                datasketches::hll_sketch hllSketch = datasketches::hll_sketch::deserialize(value.data, value.size, AggregateFunctionHllSketchAllocator());
                this->data(place).u.update(hllSketch);
            }
            else if constexpr (std::is_same_v<T, DataTypeAggregateFunction>)
            {
                //the format of this value should be the same with serialize
                const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
                if (ignore_wrong_data && value.size == 0)
                    return;
                ReadBuffer buf(const_cast<char *>(value.data), value.size);
                this->data(place).u.update(readHllSketch(buf));
            }
            else
            {
                StringRef value = columns[0]->getDataAt(row_num);
                this->data(place).u.update(value.toString());
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
        this->data(place).u.update(this->data(rhs).u.get_result());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).u.get_result().serialize_compact(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).u.update(readHllSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        if (use_composite_estimate)
            static_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).u.get_composite_estimate());
        else
            static_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).u.get_estimate());
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    bool use_composite_estimate = false;
    inline datasketches::hll_sketch readHllSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::hll_sketch::deserialize(d.data(), d.size(), AggregateFunctionHllSketchAllocator());
    }
};

template <typename T>
struct AggregateFunctionKllSketchEstimateData
{
    AggregateFunctionKllSketchEstimateData(){}

    datasketches::kll_sketch<T> u;

    static String getName() { return "kll_sketch"; }
};

template <typename T>
class AggregateFunctionKllSketchEstimate final
    : public IAggregateFunctionDataHelper<AggregateFunctionKllSketchEstimateData<T>, AggregateFunctionKllSketchEstimate<T>>
{
public:
    AggregateFunctionKllSketchEstimate(const double quantile_, const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionKllSketchEstimateData<T>, AggregateFunctionKllSketchEstimate<T>>(argument_types_, params_),quantile(quantile_), ignore_wrong_data(ignore_wrong_data_) {}

    Float64 quantile = 0;

    String getName() const override
    {
        return "kllSketchEstimate";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        try {
            const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
            if (ignore_wrong_data && value.size == 0)
                return;
            datasketches::kll_sketch<T> kll_sketch_data = datasketches::kll_sketch<T>::deserialize(value.data, value.size, datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
            this->data(place).u.merge(kll_sketch_data);
        } 
        catch (std::exception & e)
        {
            if (!ignore_wrong_data)
                throw e;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).u.merge(this->data(rhs).u);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).u.serialize(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).u.merge(readKllSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        static_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).u.get_quantile(quantile));
    }


    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    inline datasketches::kll_sketch<T> readKllSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::kll_sketch<T>::deserialize(d.data(), d.size(), datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
    }
};

template <typename T>
struct AggregateFunctionQuantilesSketchEstimateData
{
    AggregateFunctionQuantilesSketchEstimateData(){}

    datasketches::quantiles_sketch<T> u;

    static String getName() { return "quantiles_sketch"; }
};

template <typename T>
class AggregateFunctionQuantilesSketchEstimate final
    : public IAggregateFunctionDataHelper<AggregateFunctionQuantilesSketchEstimateData<T>, AggregateFunctionQuantilesSketchEstimate<T>>
{
public:
    AggregateFunctionQuantilesSketchEstimate(const double quantile_, const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionQuantilesSketchEstimateData<T>, AggregateFunctionQuantilesSketchEstimate<T>>(argument_types_, params_),quantile(quantile_), ignore_wrong_data(ignore_wrong_data_) {}

    Float64 quantile = 0;

    String getName() const override
    {
        return "quantilesSketchEstimate";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        try
        {
            const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
            if (ignore_wrong_data && value.size == 0)
                return;
            datasketches::quantiles_sketch<T> quantiles_sketch_data = datasketches::quantiles_sketch<T>::deserialize(value.data, value.size, datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
            this->data(place).u.merge(quantiles_sketch_data);
        }
        catch (std::exception & e)
        {
            if (!ignore_wrong_data)
                throw e;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).u.merge(this->data(rhs).u);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).u.serialize(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).u.merge(readQuantilesSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        static_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).u.get_quantile(quantile));
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    inline datasketches::quantiles_sketch<T> readQuantilesSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::quantiles_sketch<T>::deserialize(d.data(), d.size(), datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
    }
};

template <typename T>
class AggregateFunctionQuantilesSketchUnion final
    : public IAggregateFunctionDataHelper<AggregateFunctionQuantilesSketchEstimateData<T>, AggregateFunctionQuantilesSketchUnion<T>>
{
public:
    AggregateFunctionQuantilesSketchUnion(const DataTypes & argument_types_, const Array & params_, bool ignore_wrong_data_ = false)
        : IAggregateFunctionDataHelper<AggregateFunctionQuantilesSketchEstimateData<T>, AggregateFunctionQuantilesSketchUnion<T>>(argument_types_, params_), ignore_wrong_data(ignore_wrong_data_) {}

    String getName() const override
    {
        return "quantilesSketchUnion";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeSketchBinary>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        try
        {
            const auto & value = static_cast<const ColumnSketchBinary &>(*columns[0]).getDataAt(row_num);
            if (ignore_wrong_data && value.size == 0)
                    return;
            datasketches::quantiles_sketch<T> quantiles_sketch_data = datasketches::quantiles_sketch<T>::deserialize(value.data, value.size, datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
            this->data(place).u.merge(quantiles_sketch_data);
        }
        catch (std::exception & e)
        {
           if (!ignore_wrong_data)
                throw e;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).u.merge(this->data(rhs).u);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        std::ostringstream oss;
        this->data(place).u.serialize(oss);
        writeBinary(oss.str(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).u.merge(readQuantilesSketch(buf));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        std::ostringstream oss;
        this->data(place).u.serialize(oss);
        static_cast<ColumnSketchBinary &>(to).insert(Field(oss.str().data(), oss.str().size()));
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    bool ignore_wrong_data = false;
    inline datasketches::quantiles_sketch<T> readQuantilesSketch(ReadBuffer & buf) const
    {
        String d;
        readBinary(d, buf);
        return datasketches::quantiles_sketch<T>::deserialize(d.data(), d.size(), datasketches::serde<T>(), std::less<T>(), AggregateFunctionHllSketchAllocator());
    }
};


}

