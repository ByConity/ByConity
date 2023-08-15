#pragma once

//
// Created by likai.7@bytedance.com on 2020/05/17.
//

#include <random>
#include <algorithm>

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
struct FunctionRegAucData
{
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<std::pair<P, L>, 4096, ArenaAllocator>;
    Array pair_array;
};

template<typename P, typename L>
struct Comp{
    bool operator()(const std::pair<P, L>& a, const std::pair<P, L>& b) {
        return a.second < b.second;
    }
};

template<typename P, typename L>
class FunctionRegAuc: public IAggregateFunctionDataHelper<FunctionRegAucData<P, L>, FunctionRegAuc<P, L>>
{
private:
    UInt64 num_reg_sample; // Number of max sample count to calculate regression AUC
    UInt64 flag;

    Float64 calcRegAuc(ConstAggregateDataPtr __restrict place) const
    {
        auto & pair_array = const_cast<typename FunctionRegAucData<P, L>::Array&>(this->data(place).pair_array);

        size_t n = pair_array.size();
        size_t pair_count = n * (n - 1) / 2;
        size_t correct_count = 0;
        if (pair_count < num_reg_sample) {
            correct_count = calc_correct_pairs(pair_array, flag);
            if (flag == 4) {
                size_t label_equal_pairs = calc_label_equal_pairs(pair_array);
                pair_count -= label_equal_pairs;
            }
            if (pair_count == 0) {
                return -1.0; // Error
            } else {
                return Float64(correct_count) / pair_count;
            }

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
                if (is_correct_pair(pair_array[i], pair_array[j], flag)) {
                    correct_count++;
                }
                if (!(flag == 4 && (pair_array[i].second == pair_array[j].second))) {
                    total_count++; // final total_count may equal zero
                }
            }
            if (total_count == 0) {
                return -1.0; // Error
            } else {
                return Float64(correct_count) / total_count;
            }
        }
    }

    size_t calc_correct_pairs(PODArray<std::pair<P, L>, 4096, ArenaAllocator>& pair_array, const UInt64 p_flag) const {
        /*
            A := {(a, b)|a.first >= b.first && a.second > b.second}
            B := {(a, b)|a.first == b.first && a.second >= b.second}
            C := {(a, b)|a.first == b.first && a.second == b.second}
            D := {(a, b)|a.second == b.second}
            1. {(a, b)|(a.first > b.first && a.second > b.second) || (a.first == b.first && a.second == b.second)}
               is the default set of correct pairs, so the count of correct rate is: num(A) - (num(B)-num(C)) +  num(C) / (N*(N-1)/2)
            2. {(a, b)|(a.first > b.first && a.second > b.second) || (a.second == b.second)}
               correct rate: num(A) - (num(B) - num(C)) + num(D) / (N*(N-1)/2)
            3. {(a, b)|(a.first > b.first && a.second > b.second)}
               correct rate: num(A) - (num(B) - num(C)) / (N(N-1)/2)
            4. {(a, b)|(a.first > b.first && a.second > b.second)}
               correct rate: num(A) - (num(B) - num(C)) / (N*(N-1)/2 - D)
        */

        size_t n = pair_array.size();

        // sorted by pred asc
        std::sort(pair_array.begin(), pair_array.end());
        size_t cnt_pairs = calc_pairs(pair_array, 0, n);

        size_t i = 0, j = 0, k = 0;
        size_t cnt0 = 0; // a.first == b.first && a.second >= b.second
        size_t cnt1 = 0; // a.first == b.first && a.second == b.second
        size_t cnt2 = 0; // a.second == b.second

        for (; j < n; ++j) {
            if (pair_array[j].first != pair_array[i].first || j +1 ==n) {
                cnt0 += (j - i) * (j - i - 1) / 2;
                i = j;
            }

            if (pair_array[j] != pair_array[k] || j +1 ==n) {
                cnt1 += (j - k) * (j - k - 1) / 2;
                k = j;
            }
        }
        size_t res = 0;
        switch (p_flag) {
            case 2:
                cnt2 = calc_label_equal_pairs(pair_array);
                res = cnt_pairs + cnt1 - cnt0 + cnt2;
                break;
            case 3:
                res = cnt_pairs + cnt1 - cnt0;
                break;
            case 4:
                res = cnt_pairs + cnt1 - cnt0;
                break;
            default:
                res = cnt_pairs + 2 * cnt1 - cnt0;
        }
        return res;
    }

    size_t calc_label_equal_pairs(PODArray<std::pair<P, L>, 4096, ArenaAllocator>& pair_array) const {
        // sort by label asc
        std::sort(pair_array.begin(), pair_array.end(), Comp<P, L>());
        size_t n = pair_array.size();
        size_t i = 0, j = 0;
        size_t cnt = 0;
        for (; j < n; ++j) {
            if (pair_array[j].second != pair_array[i].second || j +1 ==n) {
                cnt += (j - i) * (j - i - 1) / 2;
                i = j;
            }
        }
        return cnt;
    }

    size_t calc_pairs(PODArray<std::pair<P, L>, 4096, ArenaAllocator>& pair_array, size_t first, size_t last) const {// [first, last)
        if (first +1 >= last) return 0;
        size_t cnt = 0;
        size_t middle = (first + last) / 2;
        cnt += calc_pairs(pair_array, first, middle);
        cnt += calc_pairs(pair_array, middle, last);
        cnt += merge_pairs(pair_array, first, middle, last);
        return cnt;
    }

    size_t merge_pairs(PODArray<std::pair<P, L>, 4096, ArenaAllocator>& pair_array, size_t first, size_t middle, size_t last) const {
        size_t cnt = 0;
        size_t i = first, j = middle;
        while (i < middle && j < last) {
            if (pair_array[i].second < pair_array[j].second) {
                i++;
            } else {
                cnt += (i - first);
                j++;
            }
        }
        if (i == middle) {
            cnt += (last - j) * (middle - first);
        }
        std::inplace_merge(pair_array.begin() + first, pair_array.begin() + middle, pair_array.begin() + last, Comp<P, L>());
        return cnt;
    }

    size_t calc_omit_pairs(PODArray<std::pair<P, L>, 4096, ArenaAllocator>& pair_array) const {
        size_t i = 0, j = 0, k = 0;
        size_t cnt0 = 0; // a.first == b.first && a.second >= b.second
        size_t cnt1 = 0; // a.first == b.first && a.second == b.second
        // size_t cnt2 = 0; // a.second == b.second
        size_t n = pair_array.size();

        // already sorted by first asc
        for (; j < n; ++j) {
            if (pair_array[j].first != pair_array[i].first || j +1 ==n) {
                cnt0 += (j - i) * (j - i - 1) / 2;
                i = j;
            }

            if (pair_array[j] != pair_array[k] || j +1 ==n) {
                cnt1 += (j - k) * (j - k - 1) / 2;
                k = j;
            }
        }

        return 2*cnt1 - cnt0;
    }

    inline bool is_correct_pair(const std::pair<P, L>& a, const std::pair<P, L>& b, const UInt64 p_flag) const {
        bool res = false;
        switch (p_flag) {
            case 1:
                res = (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second)
                    || (a.first == b.first && a.second == b.second);
                break;
            case 2:
                res = (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second)
                    || (a.second == b.second);
                break;
            case 3:
                res = (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second);
                break;
            case 4:
                res = (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second);
                break;
            default:
                res = (a.first > b.first && a.second > b.second) || (a.first < b.first && a.second < b.second)
                    || (a.first == b.first && a.second == b.second);
        }
        return res;
    }

public:
    FunctionRegAuc(const DataTypes & argument_types_,
                const UInt64 num_reg_sample_ = 1000000, const UInt64 flag_ = 1)
        : IAggregateFunctionDataHelper<FunctionRegAucData<P, L>, FunctionRegAuc<P, L>>(argument_types_, {}),
          num_reg_sample(num_reg_sample_), flag(flag_)
    {}

    String getName() const override
    { return "regression_auc"; }

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
        Float64 result = calcRegAuc(place);
        static_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }

};

}
