#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <climits>
#include <array>
#include <numeric>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Common/ArenaAllocator.h>

namespace DB {

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_ALLOCATE_MEMORY;
}

template <typename T>
using Vector = std::vector<T, TrackAllocator<T>>;

static const int TRANSFORM_TIME_GAP = 10;
static const int TRANSFORM_STEP_GAP = 10;
using Pairs = std::pair<Float64, Float64>;
using ArrayPairs = Vector<Pairs>;

using EventIdType = UInt16;
using TargetValueType = Float64;

struct ContribAnalysisResult
{
    UInt64 total_click{};
    UInt64 valid_click{};
    Vector<UInt64> transform_times{};
    Vector<UInt64> transform_steps{};
    Vector<TargetValueType> values{};

    // for touch event contribution and time/step distribution
    Vector<UInt64> transform_time_distribution{};
    Vector<UInt64> transform_step_distribution{};
    Vector<Float64> contributions{};

    // for correlation calculation
    Float64 correlation{};
    ArrayPairs features{};

    ContribAnalysisResult() 
    {
        values.resize(5); 
        contributions.resize(5); 
    }
};


template <typename AttrType>
using TouchEvent = std::pair<EventIdType, Vector<AttrType>>;
template <typename AttrType>
struct TouchEventHash {
    std::size_t operator()(const TouchEvent<AttrType>& touch_event) const {
        std::size_t seed = 0;
        seed ^= std::hash<EventIdType>{}(touch_event.first) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        for (const auto& attr : touch_event.second) {
            seed ^= std::hash<AttrType>{}(attr) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};
template <typename AttrType>
using ContribResultMap = std::unordered_map<TouchEvent<AttrType>, ContribAnalysisResult, TouchEventHash<AttrType>>;


template <typename AttrType>
void mergeContribResultMap(ContribResultMap<AttrType> & results, const ContribResultMap<AttrType> & others) 
{
    bool has_valid = std::any_of(others.begin(), others.end(), [](const auto& item) {
        return item.second.valid_click > 0;
    });

    for (const auto & item : others)
    {
        const auto & touch_event = item.first;
        auto & result = results[touch_event];

        result.total_click += item.second.total_click;
        result.valid_click += item.second.valid_click;
        result.transform_times.insert(result.transform_times.end(), item.second.transform_times.begin(), item.second.transform_times.end());
        result.transform_steps.insert(result.transform_steps.end(), item.second.transform_steps.begin(), item.second.transform_steps.end());
        
        std::transform(item.second.values.begin(), item.second.values.end(),
               result.values.begin(), result.values.begin(),
               std::plus<TargetValueType>{});

        if (has_valid)
            result.features.push_back(Pairs{item.second.total_click, item.second.valid_click});
    }
}

template <typename AttrType>
struct AggregateFunctionAttributionCorrelationData
{
    ContribResultMap<AttrType> results{};

    template <typename Type>
    void serializeVector(const Vector<Type> & vector, WriteBuffer &buf) const
    {
        writeBinary(vector.size(), buf);
        for (const auto & item : vector)
            writeBinary(item, buf);
    }

    void serializeVector(const Vector<std::pair<Float64, Float64>> & vector, WriteBuffer &buf) const
    {
        writeBinary(vector.size(), buf);
        for (const auto & item : vector)
        {
            writeBinary(item.first, buf);
            writeBinary(item.second, buf);
        }
    }

    template <typename Type>
    void deserializeVector(Vector<Type> & vector, ReadBuffer &buf) const
    {
        size_t size;
        readBinary(size, buf);
        for (size_t i = 0; i < size; i++)
        {
            Type item;
            readBinary(item, buf);
            vector.emplace_back(item);
        }
    }

    void deserializeVector(Vector<std::pair<Float64, Float64>> & vector, ReadBuffer &buf) const
    {
        size_t size;
        readBinary(size, buf);
        for (size_t i = 0; i < size; i++)
        {
            std::pair<Float64, Float64> item;
            readBinary(item.first, buf);
            readBinary(item.second, buf);
            vector.emplace_back(item);
        }
    }

    void add(ContribResultMap<AttrType> & add_results, Arena *)
    {
        mergeContribResultMap(results, add_results);
    }

    void merge(const AggregateFunctionAttributionCorrelationData &other, Arena *)
    {
        mergeContribResultMap(results, other.results);
    }

    void serialize(WriteBuffer &buf) const 
    {
        writeBinary(results.size(), buf);
        for (const auto & result : results)
        {
            writeBinary(result.first.first, buf);
            serializeVector(result.first.second, buf);

            writeBinary(result.second.total_click, buf);
            writeBinary(result.second.valid_click, buf);
            serializeVector(result.second.transform_times, buf);
            serializeVector(result.second.transform_steps, buf);
            serializeVector(result.second.values, buf);
            serializeVector(result.second.features, buf);
        }
    }

    void deserialize(ReadBuffer &buf, Arena *) 
    {
        size_t size;
        readBinary(size, buf);
        ContribResultMap<AttrType>().swap(results);

        ContribResultMap<AttrType> result_map;
        TouchEvent<AttrType> touch_event;
        for (size_t i = 0; i < size; i++) 
        {
            ContribAnalysisResult result;
            readBinary(touch_event.first, buf);
            deserializeVector(touch_event.second, buf);

            readBinary(result.total_click, buf);
            readBinary(result.valid_click, buf);
            deserializeVector(result.transform_times, buf);
            deserializeVector(result.transform_steps, buf);
            deserializeVector(result.values, buf);
            deserializeVector(result.features, buf);

            result_map[touch_event] = std::move(result);
        }
        mergeContribResultMap(results, result_map);
    }

    template <template <typename> class Comparator>
    struct ComparePairFirst final
    {
        template <typename X, typename Y>
        bool operator()(const std::pair<X, Y> & lhs, const std::pair<X, Y> & rhs) const
        {
            return Comparator<X>{}(lhs.first, rhs.first);
        }
    };

    template <template <typename> class Comparator>
    struct ComparePairSecond final
    {
        template <typename X, typename Y>
        bool operator()(const std::pair<X, Y> & lhs, const std::pair<X, Y> & rhs) const
        {
            return Comparator<Y>{}(lhs.second, rhs.second);
        }
    };

    static Float64 getRankCorrelation(ArrayPairs &value)
    {
        size_t size = value.size();

        // create a copy of values not to format data
        PODArrayWithStackMemory<std::pair<Float64, Float64>, 32> tmp_values;
        tmp_values.resize(size);
        for (size_t j = 0; j < size; ++ j)
            tmp_values[j] = static_cast<std::pair<Float64, Float64>>(value[j]);

        // sort x_values
        std::sort(std::begin(tmp_values), std::end(tmp_values), ComparePairFirst<std::greater>{});
        Float64 sumy = 0;

        for (size_t j = 0; j < size;)
        {
            // replace x_values with their ranks
            size_t rank = j + 1;
            size_t same = 1;
            size_t cur_sum = rank;
            size_t cur_start = j;

            while (j < size - 1)
            {
                if (tmp_values[j].first == tmp_values[j + 1].first)
                {
                    // rank of (j + 1)th number
                    rank += 1;
                    ++same;
                    cur_sum += rank;
                    ++j;
                }
                else
                    break;
            }

            // insert rank is calculated as average of ranks of equal values
            Float64 insert_rank = static_cast<Float64>(cur_sum) / same;
            for (size_t i = cur_start; i <= j; ++i)
                tmp_values[i].first = insert_rank;
            ++j;
        }

        // sort y_values
        std::sort(std::begin(tmp_values), std::end(tmp_values), ComparePairSecond<std::greater>{});

        // replace y_values with their ranks
        for (size_t j = 0; j < size;)
        {
            // replace x_values with their ranks
            size_t rank = j + 1;
            size_t same = 1;
            size_t cur_sum = rank;
            size_t cur_start = j;
            sumy += tmp_values[j].second;

            while (j < size - 1)
            {
                if (tmp_values[j].second == tmp_values[j + 1].second)
                {
                    // rank of (j + 1)th number
                    rank += 1;
                    ++same;
                    cur_sum += rank;
                    ++j;
                }
                else
                {
                    break;
                }
            }

            // insert rank is calculated as average of ranks of equal values
            Float64 insert_rank = static_cast<Float64>(cur_sum) / same;
            for (size_t i = cur_start; i <= j; ++i)
                tmp_values[i].second = insert_rank;
            ++j;
        }

        if (sumy == 0)
            return 0.0;

        // count d^2 sum
        Float64 answer = static_cast<Float64>(0);
        for (size_t j = 0; j < size; ++ j)
            answer += (tmp_values[j].first - tmp_values[j].second) * (tmp_values[j].first - tmp_values[j].second);

        answer *= 6;
        answer /= size * (size * size - 1);
        answer = 1 - answer;

        if (std::isnan(answer))
            return 0.0;
        return answer;
    }
};

template <typename AttrType = UInt64>
class AggregateFunctionAttributionCorrelation final
        : public IAggregateFunctionDataHelper<AggregateFunctionAttributionCorrelationData<AttrType>, AggregateFunctionAttributionCorrelation<AttrType>> {
private:
    UInt64 N; // Return the largest first N events by value
    bool need_others; // Weather need other conversion

public:

    using TouchEvent = TouchEvent<AttrType>;
    using ContribResultMap = ContribResultMap<AttrType>;

    AggregateFunctionAttributionCorrelation(
            UInt64 N_, bool need_others_,
            const DataTypes &arguments, const Array &params) :
            IAggregateFunctionDataHelper<AggregateFunctionAttributionCorrelationData<AttrType>, AggregateFunctionAttributionCorrelation>(
                    arguments, params),
            N(N_), need_others(need_others_) {}

    String getName() const override
    {
        return "attributionCorrelation";
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types;

        DataTypePtr event_id_type = std::make_shared<DataTypeUInt16>();
        DataTypePtr attr_values_type;
        if constexpr (std::is_same_v<AttrType, String>)
            attr_values_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        else
            attr_values_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypes touch_event_type{event_id_type, attr_values_type};

        DataTypePtr correlation_type = std::make_shared<DataTypeFloat64>();
        DataTypePtr click_cnt_type = std::make_shared<DataTypeUInt64>();
        DataTypePtr valid_transform_cnt_type = std::make_shared<DataTypeUInt64>();
        DataTypePtr valid_transform_ratio_type = std::make_shared<DataTypeFloat64>();
        DataTypePtr transform_times_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr transform_steps_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr value_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        DataTypePtr contribution_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        types.push_back(std::make_shared<DataTypeTuple>(touch_event_type));
        types.push_back(correlation_type);
        types.push_back(click_cnt_type);
        types.push_back(valid_transform_cnt_type);
        types.push_back(valid_transform_ratio_type);
        types.push_back(transform_times_type);
        types.push_back(transform_steps_type);
        types.push_back(value_type);
        types.push_back(contribution_type);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    template <typename TYPE>
    void getItemFromArray(Vector<TYPE>& vec, const Array& array, bool need_fixed_size = false) const
    {
        if (need_fixed_size)
            Vector<TYPE>().swap(vec);
        for(const Field& field : array)
            vec.push_back(field.get<TYPE>());
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num, Arena *arena) const override
    {
        ContribResultMap results;

        const ColumnArray* tuple_array = typeid_cast<const ColumnArray *>(columns[0]);
        const auto& field_col = static_cast<const Field &>(tuple_array->operator[](row_num));
        for(const Field& field : field_col.get<Array>())
        {
            const auto & item_tuple = field.get<Tuple>();

            TouchEvent touch_event;
            const auto & touch_event_tuple = item_tuple[0].get<Tuple>();
            touch_event.first = touch_event_tuple[0].get<UInt16>();
            getItemFromArray(touch_event.second, touch_event_tuple[1].get<Array>());

            results[touch_event].total_click = item_tuple[1].get<UInt64>();
            results[touch_event].valid_click = item_tuple[2].get<UInt64>();
            getItemFromArray(results[touch_event].transform_times, item_tuple[3].get<Array>());
            getItemFromArray(results[touch_event].transform_steps, item_tuple[4].get<Array>());
            getItemFromArray(results[touch_event].values, item_tuple[5].get<Array>(), true);
        }
        this->data(place).add(results, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer &buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer &buf, Arena *arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void getTopByValue(ContribResultMap &results) const
    {
        std::vector<std::pair<TouchEvent, ContribAnalysisResult>> result(results.begin(), results.end());
        auto cmp = [](const auto& lhs, const auto& rhs) {
            for (size_t i = 0; i < 5; ++i)
            {
                if (lhs.second.values[i] != rhs.second.values[i])
                    return lhs.second.values[i] > rhs.second.values[i];
            }
            return false;
        };
        std::partial_sort(result.begin(), result.begin() + std::min(N, result.size()), result.end(), cmp);
        result.resize(std::min(N, result.size()));
        ContribResultMap new_results;
        std::move(result.begin(), result.end(), std::inserter(new_results, new_results.end()));
        results = new_results;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        ContribResultMap & results = this->data(place).results;

        std::vector<TargetValueType> total_values(5);
        for (size_t i = 0; i < total_values.size(); ++i)
        {
            total_values[i] = std::accumulate(results.begin(), results.end(), TargetValueType{0},
                                            [i](const auto& acc, const auto& item) {
                                                return acc + item.second.values[i];
                                            });
        }

        for (auto & item : results)
        {
            ContribAnalysisResult & result = item.second;
            result.contributions.resize(result.values.size());

            for (size_t i = 0; i < total_values.size(); i++)
            {
                if (total_values[i])
                    result.contributions[i] = (result.values[i]*1.0)/total_values[i];
            }

            getDistributionByOriginal(result.transform_times, result.transform_time_distribution, TRANSFORM_TIME_GAP);
            getDistributionByOriginal(result.transform_steps, result.transform_step_distribution, TRANSFORM_TIME_GAP);

            result.correlation = AggregateFunctionAttributionCorrelationData<AttrType>::getRankCorrelation(result.features);
        }

        if (N && N < results.size())
            getTopByValue(results);

        insertResultIntoColumn(to, results);
    }

    template<typename TYPE>
    void getTopFromIndexVector(Vector<TYPE> &vec, const Vector<std::pair<Float64, UInt64>> &index_vec) const
    {
        for (size_t i = 0; i < index_vec.size(); i++)
            std::swap(vec[index_vec[i].second], vec[i]);

        vec.resize(index_vec.size());
    }

    Vector<UInt64> getMaxAndMinIndex(const Vector<UInt64> &vec) const
    {
        UInt64 max = 0, min = UINT_MAX;
        for (UInt64 i : vec)
        {
            max = (i > max) ? i : max;
            min = (i < min) ? i : min;
        }

        return Vector<UInt64>{max, min};
    }

    void getDistributionByOriginal(Vector<UInt64> & original, Vector<UInt64> & distribution, int gap_count) const
    {
        if (original.empty())
        {
            distribution.push_back(0);
            return;
        }

        Vector<UInt64> max_and_min = getMaxAndMinIndex(original);
        UInt64 gap = ceil((max_and_min[0] - max_and_min[1]) / gap_count) + 1;
        distribution.insert(distribution.begin(), gap_count, 0);

        for (UInt64 item : original)
        {
            if (item > 0)
                distribution[floor((item - max_and_min[1]) / gap)]++;
        }
    }

    template<typename ColumnNum, typename Num>
    void insertVectorNumberIntoColumn(ColumnArray& vec_to, const Vector<Num>& vec) const
    {
        auto& vec_to_offset = vec_to.getOffsets();
        vec_to_offset.push_back((vec_to_offset.empty() ? 0 : vec_to_offset.back()) + vec.size());
        auto& vec_data_to = static_cast<ColumnNum &>(vec_to.getData());
        for (const auto& item : vec)
            vec_data_to.insert(item);
    }

    template<typename ColumnType, typename Type>
    void insertVectorIntoColumn(ColumnArray& arr_to, const Vector<Type>& vec) const
    {
        auto& vec_to_offset = arr_to.getOffsets();
        vec_to_offset.push_back((vec_to_offset.empty() ? 0 : vec_to_offset.back()) + vec.size());
        auto& vec_data_to = assert_cast<ColumnType &>(arr_to.getData());
        for (const auto& item : vec)
        {
            if constexpr (std::is_same_v<Type, String>)
                vec_data_to.insertData(item.data(), item.size());
            else 
                vec_data_to.insert(item);
        }
    }

    void insertResultIntoColumn(IColumn &to, const ContribResultMap& results) const
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        ColumnTuple & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());

        ColumnTuple& touch_event_tuple_to = assert_cast<ColumnTuple &>(tuple_to.getColumn(0));
        auto & event_id_to = assert_cast<ColumnUInt16 &>(touch_event_tuple_to.getColumn(0)).getData();
        ColumnArray& attr_values_to = assert_cast<ColumnArray &>(touch_event_tuple_to.getColumn(1));

        auto & correlation_to = assert_cast<ColumnFloat64 &>(tuple_to.getColumn(1)).getData();
        auto & click_cnt_to = assert_cast<ColumnUInt64 &>(tuple_to.getColumn(2)).getData();
        auto & valid_transform_cnt_to = assert_cast<ColumnUInt64 &>(tuple_to.getColumn(3)).getData();
        auto & valid_transform_ratio_to = assert_cast<ColumnFloat64 &>(tuple_to.getColumn(4)).getData();
        ColumnArray& transform_times_to = assert_cast<ColumnArray &>(tuple_to.getColumn(5));
        ColumnArray& transform_steps_to = assert_cast<ColumnArray &>(tuple_to.getColumn(6));
        ColumnArray& value_to = assert_cast<ColumnArray &>(tuple_to.getColumn(7));
        ColumnArray& contribution_to = assert_cast<ColumnArray &>(tuple_to.getColumn(8));

        for (const auto & result : results)
        {
            event_id_to.push_back(result.first.first);
            if constexpr (std::is_same_v<AttrType, String>)
                insertVectorIntoColumn<ColumnString>(attr_values_to, result.first.second);
            else
                insertVectorIntoColumn<ColumnUInt64>(attr_values_to, result.first.second);

            correlation_to.push_back(result.second.correlation);
            click_cnt_to.push_back(result.second.total_click);
            valid_transform_cnt_to.push_back(result.second.valid_click);
            auto valid_transform_ratio = result.second.total_click ? (result.second.valid_click*1.0)/result.second.total_click : 0;
            valid_transform_ratio_to.push_back(valid_transform_ratio);

            insertVectorNumberIntoColumn<ColumnUInt64>(transform_times_to, result.second.transform_time_distribution);
            insertVectorNumberIntoColumn<ColumnUInt64>(transform_steps_to, result.second.transform_step_distribution);
            insertVectorIntoColumn<ColumnFloat64>(value_to, result.second.values);
            insertVectorIntoColumn<ColumnFloat64>(contribution_to, result.second.contributions);
        }
        offsets_to.push_back((offsets_to.empty() ? 0 : offsets_to.back()) + results.size());
    }

    bool allocatesMemoryInArena() const override { return false; }
};
}

