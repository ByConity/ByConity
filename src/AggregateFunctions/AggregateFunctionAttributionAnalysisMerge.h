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

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_ALLOCATE_MEMORY;
}

using std::vector;
using std::map;

static const int TRANSFORM_TIME_GAP = 10;
static const int TRANSFORM_STEP_GAP = 10;

struct AnalysisEnum
{
    vector<vector<String>> touch_events;
    vector<UInt64> click_cnt;
    vector<UInt64> valid_transform_cnt;
    vector<Float64> valid_transform_ratio;
    vector<vector<UInt64>> transform_times;
    vector<vector<UInt64>> transform_time_distribution;
    vector<vector<UInt64>> transform_steps;
    vector<vector<UInt64>> transform_step_distribution;
    vector<Float64> value;
    vector<Float64> contribution;
};

struct AggregateFunctionAttributionAnalysisMergeData
{
    AnalysisEnum outer_result;
    map<vector<String>, int> touch_events_map;

    void moreSpace()
    {
        outer_result.click_cnt.push_back(0);
        outer_result.valid_transform_cnt.push_back(0);
        outer_result.valid_transform_ratio.push_back(0.0);
        outer_result.transform_times.emplace_back();
        outer_result.transform_time_distribution.emplace_back();
        outer_result.transform_steps.emplace_back();
        outer_result.transform_step_distribution.emplace_back();
        outer_result.value.push_back(0.0);
        outer_result.contribution.push_back(0.0);
    }

    void integrateOuterResult(const AnalysisEnum& analysis_enum)
    {
        for (size_t i = 0; i < analysis_enum.touch_events.size(); i++)
        {
            vector<String> key = vector{analysis_enum.touch_events[i][0], analysis_enum.touch_events[i][1]};
            if (!touch_events_map.count(key))
            {
                touch_events_map.insert(make_pair(key, touch_events_map.size()));
                outer_result.touch_events.push_back(key);
                moreSpace();
            }

            int index = touch_events_map[analysis_enum.touch_events[i]];
            outer_result.click_cnt[index] += analysis_enum.click_cnt[i];
            outer_result.valid_transform_cnt[index] += analysis_enum.valid_transform_cnt[i];
            outer_result.value[index] += analysis_enum.value[i];

            if (!analysis_enum.transform_times.empty() && !analysis_enum.transform_times[i].empty())
                outer_result.transform_times[index].insert(outer_result.transform_times[index].end(), analysis_enum.transform_times[i].begin(), analysis_enum.transform_times[i].end());

            if (!analysis_enum.transform_steps.empty() && !analysis_enum.transform_steps[i].empty())
                outer_result.transform_steps[index].insert(outer_result.transform_steps[index].end(), analysis_enum.transform_steps[i].begin(), analysis_enum.transform_steps[i].end());
        }
    }

    void add(const AnalysisEnum& analysis_enum, Arena *)
    {
        integrateOuterResult(analysis_enum);
    }

    void merge(const AggregateFunctionAttributionAnalysisMergeData & other, Arena *)
    {
        integrateOuterResult(other.outer_result);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(touch_events_map.size(), buf);
        for (const auto &map : touch_events_map)
        {
            writeBinary(map.first, buf);
            writeBinary(map.second, buf);
        }

        writeBinary(outer_result.touch_events, buf);
        writeBinary(outer_result.click_cnt, buf);
        writeBinary(outer_result.valid_transform_cnt, buf);
        writeBinary(outer_result.transform_times, buf);
        writeBinary(outer_result.transform_steps, buf);
        writeBinary(outer_result.value, buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size;
        readBinary(size, buf);

        String touch_event;
        String event_attribute;
        int index;
        touch_events_map.clear();
        for (size_t i = 0; i < size; i++)
        {
            readBinary(touch_event, buf);
            readBinary(index, buf);
            touch_events_map.insert(make_pair(vector<String>{touch_event, event_attribute}, index));
        }

        readBinary(outer_result.touch_events, buf);
        readBinary(outer_result.click_cnt, buf);
        readBinary(outer_result.valid_transform_cnt, buf);
        readBinary(outer_result.transform_times, buf);
        readBinary(outer_result.transform_steps, buf);
        readBinary(outer_result.value, buf);
    }
};

class AggregateFunctionAttributionAnalysisTupleMerge final : public IAggregateFunctionDataHelper<AggregateFunctionAttributionAnalysisMergeData, AggregateFunctionAttributionAnalysisTupleMerge>
{
private:
    UInt64 N; // Return the largest first N events by value
    bool need_others; // Weather need other conversion

public:
    AggregateFunctionAttributionAnalysisTupleMerge(
        UInt64 N_, bool need_others_,
        const DataTypes & arguments, const Array & params) :
        IAggregateFunctionDataHelper<AggregateFunctionAttributionAnalysisMergeData, AggregateFunctionAttributionAnalysisTupleMerge>(arguments, params),
        N(N_), need_others(need_others_) {}

    String getName() const override
    {
        return "attributionAnalysisMerge";
    }

    DataTypePtr getAttributionAnalysisReturnType() const
    {
        DataTypes types;
        DataTypePtr touch_events_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
        DataTypePtr click_cnt_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr valid_transform_cnt_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr valid_transform_ratio_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        DataTypePtr transform_times_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
        DataTypePtr transform_steps_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
        DataTypePtr contribution_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        DataTypePtr value_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        types.push_back(touch_events_type);
        types.push_back(click_cnt_type);
        types.push_back(valid_transform_cnt_type);
        types.push_back(valid_transform_ratio_type);
        types.push_back(transform_times_type);
        types.push_back(transform_steps_type);
        types.push_back(value_type);
        types.push_back(contribution_type);

        return std::make_shared<DataTypeTuple>(types);
    }

    DataTypePtr getReturnType() const override
    {
        return getAttributionAnalysisReturnType();
    }

    template <typename TYPE>
    void transformArrayIntoVector(vector<TYPE>& vec, const ColumnArray* columnArray, size_t row_num) const
    {
        const auto& field_col = static_cast<const Field &>(columnArray->operator[](row_num));
        for(const Field& field : field_col.get<Array>())
            vec.push_back(field.get<TYPE>());
    }

    template <typename TYPE>
    void transformArrayIntoNestedVector(vector<vector<TYPE>>& vec, const ColumnArray* columnArray, size_t row_num) const
    {
        const auto& field_col = static_cast<const Field &>(columnArray->operator[](row_num));
        for (const Field& field : field_col.get<Array>())
        {
            vector<TYPE> res;
            for (const Field& f : field.get<Array>())
                res.push_back(f.get<TYPE>());

            vec.push_back(res);
        }
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num, Arena *arena) const override
    {
        AnalysisEnum analysis_enum;

        const auto* input = (typeid_cast<const ColumnTuple *>(columns[0]));
        const auto & tuple_inputs = input->getColumns();

        if (tuple_inputs.size() != 7) return;

        const ColumnArray* touch_events_col = typeid_cast<const ColumnArray *>(tuple_inputs[0].get());
        const ColumnArray* click_cnt_col = typeid_cast<const ColumnArray *>(tuple_inputs[1].get());
        const ColumnArray* valid_transform_cnt_col = typeid_cast<const ColumnArray *>(tuple_inputs[2].get());
        const ColumnArray* transform_times_col = typeid_cast<const ColumnArray *>(tuple_inputs[3].get());
        const ColumnArray* transform_steps_col = typeid_cast<const ColumnArray *>(tuple_inputs[4].get());
        const ColumnArray* value_col = typeid_cast<const ColumnArray *>(tuple_inputs[5].get());
        const ColumnArray* contribution_col = typeid_cast<const ColumnArray *>(tuple_inputs[6].get());

        transformArrayIntoNestedVector(analysis_enum.touch_events, touch_events_col, row_num);
        transformArrayIntoVector(analysis_enum.click_cnt, click_cnt_col, row_num);
        transformArrayIntoVector(analysis_enum.valid_transform_cnt, valid_transform_cnt_col, row_num);
        transformArrayIntoNestedVector(analysis_enum.transform_times, transform_times_col, row_num);
        transformArrayIntoNestedVector(analysis_enum.transform_steps, transform_steps_col, row_num);
        transformArrayIntoVector(analysis_enum.value, value_col, row_num);
        transformArrayIntoVector(analysis_enum.contribution, contribution_col, row_num);

        this->data(place).add(analysis_enum, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer &buf, Arena *arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        AnalysisEnum outer_result = this->data(place).outer_result;

        Float64 total_value = std::accumulate(outer_result.value.begin() , outer_result.value.end() , 0.0);
        if (total_value > 0)
        {
            for (size_t i = 0; i < outer_result.touch_events.size(); i++)
            {
                outer_result.valid_transform_ratio[i] =
                    (outer_result.click_cnt[i] != 0) ?
                    (outer_result.valid_transform_cnt[i]*1.0) / (outer_result.click_cnt[i]*1.0) : 0;
                outer_result.contribution[i] = outer_result.value[i] / total_value;
            }
        }
        getDistributionByOriginal(outer_result.transform_times, outer_result.transform_time_distribution, TRANSFORM_TIME_GAP);
        getDistributionByOriginal(outer_result.transform_steps, outer_result.transform_step_distribution, TRANSFORM_STEP_GAP);

        if(N && N < outer_result.touch_events.size()) getTopByValue(outer_result);

        insertResultIntoColumn(to, outer_result);
    }

    template<typename TYPE>
    void getTopFromIndexVector(vector<TYPE>& vec, const vector<std::pair<Float64, UInt64>>& index_vec) const
    {
        for (size_t i = 0; i < index_vec.size(); i++)
            std::swap(vec[index_vec[i].second], vec[i]);

        vec.resize(index_vec.size());
    }

    void getTopByValue(AnalysisEnum& outer_result) const
    {
        vector<std::pair<Float64, UInt64>> index_vec;
        int other_index = -1;
        for (size_t i = 0; i < outer_result.value.size(); i++)
        {
            if (outer_result.touch_events[i][0] == "$other_conversions")
            {
                other_index = i;
                continue;
            }
            index_vec.emplace_back(outer_result.value[i], i);
        }

        std::nth_element(index_vec.begin(), index_vec.begin()+N, index_vec.end(),
                         [](std::pair<Float64, UInt64> value1, std::pair<Float64, UInt64> value2) {
                             return value1.first != value2.first ? value1.first > value2.first : value1.second < value2.second;
                         });
        index_vec.resize(N);

        if (need_others && other_index > -1)
            index_vec.emplace_back(outer_result.value[other_index], other_index);

        std::sort(index_vec.begin(), index_vec.end(), [](auto& i1, auto& i2) {
            return i1.second < i2.second;
        });
        getTopFromIndexVector(outer_result.touch_events, index_vec);
        getTopFromIndexVector(outer_result.click_cnt, index_vec);
        getTopFromIndexVector(outer_result.valid_transform_cnt, index_vec);
        getTopFromIndexVector(outer_result.valid_transform_ratio, index_vec);
        getTopFromIndexVector(outer_result.transform_time_distribution, index_vec);
        getTopFromIndexVector(outer_result.transform_step_distribution, index_vec);
        getTopFromIndexVector(outer_result.value, index_vec);
        getTopFromIndexVector(outer_result.contribution, index_vec);
    }

    void getDistributionByOriginal(const vector<vector<UInt64>> & original, vector<vector<UInt64>> & distribution, int gap_count) const
    {
        for (size_t i = 0; i < original.size(); i++)
        {
            if (original[i].empty())
            {
                distribution[i].push_back(0);
                return;
            }

            vector<UInt64> max_and_min = getMaxAndMinIndex(original[i]);

            UInt64 gap = ceil((max_and_min[0]-max_and_min[1]) / gap_count) + 1;
            distribution[i].insert(distribution[i].begin(), gap_count, 0);
            for (UInt64 item : original[i])
            {
                if (item > 0)
                    distribution[i][floor((item-max_and_min[1])/gap)]++;
            }
        }
    }

    vector<UInt64> getMaxAndMinIndex(const vector<UInt64>& vec) const
    {
        UInt64 max = 0, min = UINT_MAX;
        for (UInt64 i : vec)
        {
            max = (i > max) ? i : max;
            min = (i < min) ? i : min;
        }
        return vector<UInt64>{max, min};
    }

    template<typename ColumnNum, typename Num>
    void insertNestedVectorNumberIntoColumn(ColumnArray& vec_to, const vector<Num>& vec) const
    {
        auto& vec_to_offset = vec_to.getOffsets();
        vec_to_offset.push_back((vec_to_offset.empty() ? 0 : vec_to_offset.back()) + vec.size());
        auto& vec_to_nest = static_cast<ColumnArray &>(vec_to.getData());
        auto& vec_data_to = static_cast<ColumnNum &>(static_cast<ColumnArray &>(vec_to_nest).getData());
        auto& vec_to_nest_offset = vec_to_nest.getOffsets();
        for (const auto& item : vec)
        {
            for (const auto& i : item)
                vec_data_to.insert(i);

            vec_to_nest_offset.push_back((vec_to_nest_offset.empty() ? 0 : vec_to_nest_offset.back()) + item.size());
        }
    }

    template<typename ColumnNum, typename Num>
    void insertVectorNumberIntoColumn(ColumnArray& vec_to, const vector<Num>& vec) const
    {
        auto& vec_to_offset = vec_to.getOffsets();
        vec_to_offset.push_back((vec_to_offset.empty() ? 0 : vec_to_offset.back()) + vec.size());
        auto& vec_data_to = static_cast<ColumnNum &>(vec_to.getData());
        for (const auto& item : vec)
            vec_data_to.insert(item);
    }

    void insertResultIntoColumn(IColumn &to, const AnalysisEnum& result) const
    {
        ColumnTuple & tuple_to = static_cast<ColumnTuple &>(to);

        ColumnArray& touch_events_to = static_cast<ColumnArray &>(tuple_to.getColumn(0));
        auto& touch_events_to_offset = touch_events_to.getOffsets();
        touch_events_to_offset.push_back((touch_events_to_offset.empty() ? 0 : touch_events_to_offset.back()) + result.touch_events.size());
        auto& touch_events_to_nest = static_cast<ColumnArray &>(touch_events_to.getData());
        auto &touch_events_to_nest_offset = touch_events_to_nest.getOffsets();
        auto& touch_events_data_to = static_cast<ColumnString &>(touch_events_to_nest.getData());
        for (const auto& item : result.touch_events)
        {
            for (const auto& s : item) touch_events_data_to.insertData(s.data(), s.size());
            touch_events_to_nest_offset.push_back((touch_events_to_nest_offset.empty() ? 0 : touch_events_to_nest_offset.back()) + item.size());
        }

        auto& click_cnt_to = static_cast<ColumnArray &>(tuple_to.getColumn(1));
        insertVectorNumberIntoColumn<ColumnUInt64>(click_cnt_to, result.click_cnt);

        ColumnArray& valid_transform_cnt_to = static_cast<ColumnArray &>(tuple_to.getColumn(2));
        insertVectorNumberIntoColumn<ColumnUInt64>(valid_transform_cnt_to, result.valid_transform_cnt);

        ColumnArray& valid_transform_ratio_to = static_cast<ColumnArray &>(tuple_to.getColumn(3));
        insertVectorNumberIntoColumn<ColumnFloat64>(valid_transform_ratio_to, result.valid_transform_ratio);

        ColumnArray& transform_time_distribution_to = static_cast<ColumnArray &>(tuple_to.getColumn(4));
        insertNestedVectorNumberIntoColumn<ColumnUInt64>(transform_time_distribution_to, result.transform_time_distribution);

        ColumnArray& transform_step_distribution_to = static_cast<ColumnArray &>(tuple_to.getColumn(5));
        insertNestedVectorNumberIntoColumn<ColumnUInt64>(transform_step_distribution_to, result.transform_step_distribution);

        ColumnArray& value_to = static_cast<ColumnArray &>(tuple_to.getColumn(6));
        insertVectorNumberIntoColumn<ColumnFloat64>(value_to, result.value);

        ColumnArray& contribution_to = static_cast<ColumnArray &>(tuple_to.getColumn(7));
        insertVectorNumberIntoColumn<ColumnFloat64>(contribution_to, result.contribution);
    }

    bool allocatesMemoryInArena() const override { return false; }
};
}

