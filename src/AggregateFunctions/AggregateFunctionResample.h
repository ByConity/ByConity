#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/assert_cast.h>
#include <common/arithmeticOverflow.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

template <typename Key>
class AggregateFunctionResample final : public IAggregateFunctionHelper<AggregateFunctionResample<Key>>
{
private:
    /// Sanity threshold to avoid creation of too large arrays. The choice of this number is arbitrary.
    const size_t MAX_ELEMENTS = 1048576;

    AggregateFunctionPtr nested_function;

    size_t last_col;

    Key begin;
    Key end;
    size_t step;

    size_t total;
    size_t align_of_data;
    size_t size_of_data;

public:
    AggregateFunctionResample(
        AggregateFunctionPtr nested_function_,
        Key begin_,
        Key end_,
        size_t step_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionResample<Key>>{arguments, params}
        , nested_function{nested_function_}
        , last_col{arguments.size() - 1}
        , begin{begin_}
        , end{end_}
        , step{step_}
        , total{0}
        , align_of_data{nested_function->alignOfData()}
        , size_of_data{(nested_function->sizeOfData() + align_of_data - 1) / align_of_data * align_of_data}
    {
        // notice: argument types has been checked before
        if (step == 0)
            throw Exception("The step given in function "
                    + getName() + " should not be zero",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (end < begin)
            total = 0;
        else
        {
            Key dif;
            size_t sum;
            if (common::subOverflow(end, begin, dif)
                || common::addOverflow(static_cast<size_t>(dif), step, sum))
            {
                throw Exception("Overflow in internal computations in function " + getName()
                    + ". Too large arguments", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }

            total = (sum - 1) / step; // total = (end - begin + step - 1) / step
        }

        if (total > MAX_ELEMENTS)
            throw Exception("The range given in function "
                    + getName() + " contains too many elements",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    String getName() const override
    {
        return nested_function->getName() + "Resample";
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return total * size_of_data;
    }

    size_t alignOfData() const override
    {
        return align_of_data;
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        for (size_t i = 0; i < total; ++i)
        {
            try
            {
                nested_function->create(place + i * size_of_data);
            }
            catch (...)
            {
                for (size_t j = 0; j < i; ++j)
                    nested_function->destroy(place + j * size_of_data);
                throw;
            }
        }
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroy(place + i * size_of_data);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->destroyUpToState(place + i * size_of_data);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena) const override
    {
        Key key;

        if constexpr (static_cast<Key>(-1) < 0)
            key = columns[last_col]->getInt(row_num);
        else
            key = columns[last_col]->getUInt(row_num);

        if (key < begin || key >= end)
            return;

        size_t pos = (key - begin) / step;

        nested_function->add(place + pos * size_of_data, columns, row_num, arena);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->merge(place + i * size_of_data, rhs + i * size_of_data, arena);
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->serialize(place + i * size_of_data, buf);
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena) const override
    {
        for (size_t i = 0; i < total; ++i)
            nested_function->deserialize(place + i * size_of_data, buf, arena);
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(nested_function->getReturnType());
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        for (size_t i = 0; i < total; ++i)
        {
            if constexpr (merge)
                nested_function->insertMergeResultInto(place + i * size_of_data, col.getData(), arena);
            else
                nested_function->insertResultInto(place + i * size_of_data, col.getData(), arena);
        }

        col_offsets.getData().push_back(col.getData().size());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
