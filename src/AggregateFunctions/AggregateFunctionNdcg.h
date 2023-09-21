#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/PODArray.h>
#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>

#include <unordered_map>
#include <vector>

namespace DB
{
    template<typename I, typename R, typename P, typename L>
    struct DataEntry
    {
        I rank_id;
        R rank;
        P pred;
        L label;

        DataEntry(const I & rank_id_, const R & rank_, const P & pred_, const L & label_) : rank_id(rank_id_), rank(rank_), pred(pred_), label(label_) {}
    };

    template<typename I, typename R, typename P, typename L>
    struct AggregateFunctionNdcgData
    {
        using Allocator = MixedArenaAllocator<4096>;
        using Array = PODArray<DataEntry<I, R, P, L>, 4096, ArenaAllocator>;
        Array data_array;
    };

    template<typename I, typename R, typename P, typename L>
    class AggregateFunctionNdcg final : public IAggregateFunctionDataHelper<AggregateFunctionNdcgData<I, R, P, L>, AggregateFunctionNdcg<I, R, P, L>>
    {
    public:
        AggregateFunctionNdcg(const DataTypes & argument_types_)
                : IAggregateFunctionDataHelper<AggregateFunctionNdcgData<I, R, P, L>, AggregateFunctionNdcg<I, R, P, L>>(argument_types_, {})
                {}

        String getName() const override
        {
            return "ndcg";
        }

        DataTypePtr getReturnType() const override
        {
            return std::make_shared<DataTypeFloat64>();
        }

        bool allocatesMemoryInArena() const override { return false; }

        void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
        {
            auto & rank_id = static_cast<const ColumnVector<I> &>(*columns[0]).getData()[row_num];
            auto & rank = static_cast<const ColumnVector<R> &>(*columns[1]).getData()[row_num];
            auto & pred = static_cast<const ColumnVector<P> &>(*columns[2]).getData()[row_num];
            auto & label = static_cast<const ColumnVector<L> &>(*columns[3]).getData()[row_num];

            DataEntry<I, R, P, L> entry(rank_id, rank, pred, label);
            this->data(place).data_array.push_back(entry, arena);
        }

        void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
        {
            auto & rhs_data = this->data(rhs);
            this->data(place).data_array.insert(rhs_data.data_array.begin(), rhs_data.data_array.end(), arena);
        }

        void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
        {
            auto & data_array = this->data(place).data_array;
            auto size = data_array.size();
            writeVarUInt(size, buf);
            buf.write(reinterpret_cast<const char *>(&data_array[0]), size * sizeof(data_array[0]));
        }

        void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
        {
            size_t size = 0;
            readVarUInt(size, buf);

            auto & data_array = this->data(place).data_array;
            data_array.resize(size, arena);
            buf.read(reinterpret_cast<char *>(&data_array[0]), size * sizeof(data_array[0]));
        }

        void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
        {
            Float64 result = calcNdcg(place);
            static_cast<ColumnFloat64 &>(to).getData().push_back(result);
        }

    private:
        Float64 calcNdcg(ConstAggregateDataPtr place) const
        {
            auto & data_array = this->data(place).data_array;
            std::unordered_map<I, std::vector<DataEntry<I, R, P, L>>> rank_map;

            for (auto & entry : data_array)
            {
                if (rank_map.find(entry.rank_id) == rank_map.end())
                    rank_map[entry.rank_id] = std::vector<DataEntry<I, R, P, L>>();
                rank_map[entry.rank_id].emplace_back(entry);
            }

            Float64 global_ndcg = 0;
            for (auto & item : rank_map)
            {
                auto & rank_vec = item.second;
                std::sort(rank_vec.begin(), rank_vec.end(),
                          [](const DataEntry<I, R, P, L> & a, const DataEntry<I, R, P, L> & b) -> bool
                          {
                              return a.pred > b.pred;
                          });

                auto size = rank_vec.size();
                std::vector<R> real_rank(size);
                for (size_t i = 0; i < size; ++i)
                    real_rank[i] = rank_vec[i].rank;
                std::sort(real_rank.begin(), real_rank.end());

                Float64 dcg = 0;
                Float64 idcg = 0;
                for (size_t i = 0; i < size; ++i) {
                    auto & entry = rank_vec[i];
                    Float64 rel = (1 << Int64(entry.label + 1)) - 1;
                    idcg += rel / std::log2(Float64(entry.rank + 2));
                    dcg += rel / std::log2(Float64(real_rank[i] + 2));
                }
                global_ndcg += dcg / idcg;
            }

            global_ndcg /= rank_map.size();
            return global_ndcg;
        }
    };
}
