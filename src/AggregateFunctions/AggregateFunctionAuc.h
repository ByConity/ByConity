#pragma once

#include <random>

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

template<typename P, typename L>
struct FunctionAucData
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<std::pair<P, L>, 4096, ArenaAllocator>;
    Array pair_array;
};

template<typename P, typename L>
class FunctionAuc: public IAggregateFunctionDataHelper<FunctionAucData<P, L>, FunctionAuc<P, L>>
{
private:
    bool is_regression; // If to calculate regression AUC
    UInt64 num_reg_sample; // Number of max sample count to calculate regression AUC

    Float64 calcAuc(ConstAggregateDataPtr __restrict place) const
    {
        auto & pair_array = const_cast<typename FunctionAucData<P, L>::Array&>(this->data(place).pair_array);
        if (!is_regression) {
            // Calculate AUC for binary classification model
            std::sort(pair_array.begin(), pair_array.end());
            size_t sum_rank_mul2 = 0;
            size_t num_pos = 0;
            for (size_t i = 0; i < pair_array.size(); ++i)
            {
                const auto &cur_pair = pair_array[i];
                size_t total_rank = i;
                size_t pos_weight = pair_array[i].second > 1e-6;
                size_t neg_weight = pair_array[i].second <= 1e-6;
                while (i + 1 < pair_array.size() && pair_array[i + 1].first <= cur_pair.first) {
                  pos_weight += (pair_array[i + 1].second > 1e-6);
                  neg_weight += (pair_array[i + 1].second <= 1e-6);
                  ++i;
                }
                // for repeated predict value, same with AggregateFunctionFastAuc2.h#L58
                // multiply 2 to avoid precision loss of (total_rank + (pos_weight + neg_weight + 1) / 2.0)
                sum_rank_mul2 += (total_rank * 2 + (pos_weight + neg_weight + 1)) * pos_weight;
                num_pos += pos_weight;
            }
            size_t num_neg = pair_array.size() - num_pos;
            Float64 result = 0.0;
            if (num_neg == 0 || num_pos == 0)
                result = 1.0;
            else
                result = (sum_rank_mul2 - num_pos * (num_pos + 1)) / Float64(num_pos) / Float64(num_neg) / Float64(2);
            return result;
        } else {
            // Calculate AUC for regression model
            size_t n = pair_array.size();
            size_t pair_count = n * (n - 1) / 2;
            size_t correct_count = 0;
            if (pair_count < num_reg_sample) { // If the total pairs is small, enumerate all pairs
                for (size_t i = 0; i < n; ++i) {
                    for (size_t j = i + 1; j < n; ++j) {
                        if (is_correct_pair(pair_array[i], pair_array[j])) {
                            correct_count++;
                        }
                    }
                }
                return Float64(correct_count) / pair_count;
            } else { // Otherwise, sample `num_reg_sample` pairs
                size_t total_count = 0, attempt_count = 0;
                std::random_device r;
                std::default_random_engine generator(r());
                std::uniform_int_distribution<size_t> distribution(0, n-1);
                while (total_count < num_reg_sample && attempt_count < num_reg_sample * 2) {
                    attempt_count++;
                    size_t i = distribution(generator);
                    size_t j = distribution(generator);
                    if (i == j) {
                        continue;
                    }
                    if (is_correct_pair(pair_array[i], pair_array[j])) {
                        correct_count++;
                    }
                    total_count++;
                }
                return Float64(correct_count) / total_count;
            }
        }
    }

    inline bool is_correct_pair(const std::pair<P, L>& a, const std::pair<P, L>& b) const {
        return (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second)
            || (a.first == b.first && a.second == b.second);
    }

public:
    FunctionAuc(const DataTypes & argument_types_,
                const bool is_regression_ = false,
                const UInt64 num_reg_sample_ = 1000000)
        : IAggregateFunctionDataHelper<FunctionAucData<P, L>, FunctionAuc<P, L>>(argument_types_, {}),
          is_regression(is_regression_),
          num_reg_sample(num_reg_sample_)
    {}

    String getName() const override
    { return "auc"; }

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeFloat64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto p = std::make_pair(static_cast<const ColumnVector<P> &>(*columns[0]).getData()[row_num],
                     static_cast<const ColumnVector<L> &>(*columns[1]).getData()[row_num]);
        this->data(place).pair_array.push_back(p, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & lhs_data = this->data(place);
        auto & rhs_data = this->data(rhs);
        lhs_data.pair_array.insert(rhs_data.pair_array.begin(), rhs_data.pair_array.end(), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        const auto & pair_array = this->data(place).pair_array;
        size_t size = pair_array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&pair_array[0]), size * sizeof(pair_array[0]));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        auto & pair_array = this->data(place).pair_array;
        pair_array.resize(size, arena);
        buf.read(reinterpret_cast<char *>(&pair_array[0]), size * sizeof(pair_array[0]));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = calcAuc(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }
};

}
