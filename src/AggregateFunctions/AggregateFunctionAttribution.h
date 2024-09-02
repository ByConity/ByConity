#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <numeric>
#include <type_traits>
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
#include "DataTypes/DataTypeNullable.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
using Vector = std::vector<T, TrackAllocator<T>>;

enum class EventType
{
    TOUCH_EVENT,
    PROCEDURE_EVENT,
    TARGET_EVENT
};

using EventIdType = UInt16;
using TargetValueType = Float64;

template <typename TimeType, typename AttrType>
struct AttrAnalysisEvent
{
    EventType event_type;
    TimeType time;
    EventIdType event_id;
    Vector<AttrType> attr_values;
    // Only used when evnet type is TARGET_EVENT
    TargetValueType target_value{};

    AttrAnalysisEvent() = default;
    AttrAnalysisEvent(EventType event_type_, TimeType time_, EventIdType event_id_, Vector<AttrType> & attr_values_, TargetValueType target_value_): 
        event_type(event_type_), time(time_), event_id(event_id_), attr_values(std::move(attr_values_)), target_value(target_value_) {}

    bool operator < (const AttrAnalysisEvent & event) const
    {
        return time < event.time || (time == event.time && event_id < event.event_id);
    }

    void eventSerialize(WriteBuffer & buf) const
    {
        writeBinary(UInt8(event_type), buf);
        writeBinary(time, buf);
        writeBinary(event_id, buf);
        writeFloatBinary(target_value, buf);

        writeBinary(attr_values.size(), buf);
        for (const auto & attr : attr_values)
            writeBinary(attr, buf);
    }

    void eventDeserialize(ReadBuffer & buf)
    {
        UInt8 etype;
        readBinary(etype, buf);
        event_type = EventType(etype);
        readBinary(time, buf);
        readBinary(event_id, buf);
        readFloatBinary(target_value, buf);

        size_t size;
        readBinary(size, buf);
        attr_values.clear();
        for (size_t i = 0; i < size; i++)
        {
            AttrType attr;
            readBinary(attr, buf);
            attr_values.emplace_back(attr);
        }
    }
};

struct AnalysisResult
{
    UInt64 total_click{};
    UInt64 valid_click{};
    Vector<UInt64> transform_times{};
    Vector<UInt64> transform_steps{};
    Vector<TargetValueType> value{};

    AnalysisResult() { value.resize(5); }
};

template <typename TimeType, typename AttrType>
struct AggregateFunctionAttributionData
{
    using Event = AttrAnalysisEvent<TimeType, AttrType>;
    using Events = Vector<AttrAnalysisEvent<TimeType, AttrType>>;
    Events events;

    void add(EventType event_type, TimeType time, EventIdType event_id, Vector<AttrType> & attr_values, TargetValueType target_value, Arena *)
    {
        events.emplace_back(Event{event_type, time, event_id, attr_values, target_value});
    }

    void merge(const AggregateFunctionAttributionData & other, Arena *)
    {
        events.insert(events.end(), other.events.begin(), other.events.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(events.size(), buf);
        for (const auto & event : events)
            event.eventSerialize(buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size;
        readBinary(size, buf);
        Events().swap(events);

        AttrAnalysisEvent<TimeType, AttrType> event;
        for (size_t i = 0; i < size; ++i)
        {
            event.eventDeserialize(buf);
            events.insert(lower_bound(events.begin(), events.end(), event), event);
        }
    }
};

template <typename TimeType, typename AttrType>
class AggregateFunctionAttribution final: public IAggregateFunctionDataHelper<AggregateFunctionAttributionData<TimeType, AttrType>, AggregateFunctionAttribution<TimeType, AttrType>>
{
private:

    std::vector<UInt8> events_flag;
    UInt8 need_group;
    std::vector<UInt16> attr_relation_matrix;

    UInt8 mode_flag;
    UInt64 t = 1;
    double o = 0.4, p = 0.2, q = 0.4;

    bool other_transform;

    UInt64 window;
    const DateLUTImpl & date_lut;

public:

    using Event = AttrAnalysisEvent<TimeType, AttrType>;
    using Events = Vector<Event>;
    using MultipleEvents = Vector<Events>;
    using TouchEvent = std::pair<EventIdType, Vector<AttrType>>;
    struct TouchEventHash {
        std::size_t operator()(const TouchEvent& touch_event) const {
            std::size_t seed = 0;
            seed ^= std::hash<EventIdType>{}(touch_event.first) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            for (const auto& attr : touch_event.second) {
                seed ^= std::hash<AttrType>{}(attr) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }
    };
    using ResultMap = std::unordered_map<TouchEvent, AnalysisResult, TouchEventHash>;

    AggregateFunctionAttribution(
        std::vector<UInt8> & events_flag_, UInt8 need_group_, std::vector<UInt16> & attr_relation_matrix_,
        UInt8 mode_flag_, std::vector<UInt64> attribution_args_, 
        bool other_transform_, UInt64 window_, String time_zone_,
        const DataTypes & arguments, const Array & params) :
        IAggregateFunctionDataHelper<AggregateFunctionAttributionData<TimeType, AttrType>, AggregateFunctionAttribution<TimeType, AttrType>>(arguments, params),
        events_flag(events_flag_), need_group(need_group_), attr_relation_matrix(std::move(attr_relation_matrix_)),
        mode_flag(mode_flag_), other_transform(other_transform_),
        window(window_), date_lut(DateLUT::instance(time_zone_))
    {
        if (attribution_args_.empty()) return;
        if (attribution_args_.size() >= 3)
        {
            o = attribution_args_[0]*0.01;
            p = attribution_args_[1]*0.01;
            q = attribution_args_[2]*0.01;
        }
        t = (attribution_args_.size() == 4) ? attribution_args_[3] : attribution_args_[0];
    }

    String getName() const override
    {
        return "attribution";
    }

    bool handleNullItSelf() const override
    {
        return true;
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

        DataTypePtr click_cnt_type = std::make_shared<DataTypeUInt64>();
        DataTypePtr valid_transform_cnt_type = std::make_shared<DataTypeUInt64>();
        DataTypePtr transform_times_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr transform_steps_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
        DataTypePtr value_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        types.push_back(std::make_shared<DataTypeTuple>(touch_event_type));
        types.push_back(click_cnt_type);
        types.push_back(valid_transform_cnt_type);
        types.push_back(transform_times_type);
        types.push_back(transform_steps_type);
        types.push_back(value_type);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    inline EventType getEventType(EventIdType id, const std::vector<UInt8> & events_flag_) const
    {
        return (id <= events_flag_[0]) ? EventType::TOUCH_EVENT :
               (id <= events_flag_[1]) ? EventType::PROCEDURE_EVENT :
                                        EventType::TARGET_EVENT;
    }

    inline EventIdType getProcedureNum(EventIdType id, const std::vector<UInt8> & events_flag_) const
    {
        return id - events_flag_[0];
    }

    Vector<AttrType> getAttrValues(EventIdType event_id, EventType type, const IColumn** columns, size_t row_num) const
    {
        auto get_related_attr = [&](size_t index)
        {
            // 3: The number of columns before the attribute-related columns (respectively time, event_id, target_value)
            index = index + 4;
            if constexpr (std::is_same_v<AttrType, String>)
                return (static_cast<const ColumnString *>(columns[index]))->getDataAt(row_num).toString();
            else
                return static_cast<const ColumnVector<AttrType> *>(columns[index])->getData()[row_num];
        };

        Vector<AttrType> attr_values;
        if (need_group && type == EventType::TOUCH_EVENT)
        {
            const ColumnArray* array_type = typeid_cast<const ColumnArray *>(columns[3]);
            const auto& field_col = static_cast<const Field &>(array_type->operator[](row_num));
            for (const Field & field : field_col.get<Array>())
                attr_values.push_back(field.get<AttrType>());
        }
        else if (!attr_relation_matrix.empty() && type == EventType::PROCEDURE_EVENT)
        {
            size_t index = 2 * (getProcedureNum(event_id, events_flag) - 1);
            if (index >= attr_relation_matrix.size()) 
                throw Exception("Index " + std::to_string(index) + "in attr_list is out of boundary " + std::to_string(attr_relation_matrix.size()), ErrorCodes::BAD_ARGUMENTS);
            attr_values.push_back(get_related_attr(attr_relation_matrix[index]));
        }
        else if (type == EventType::TARGET_EVENT)
        {
            for (size_t i = 1; i < attr_relation_matrix.size(); i+=2)
                attr_values.push_back(get_related_attr(attr_relation_matrix[i]));
        }
        return attr_values;
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num, Arena * arena) const override
    {
        TimeType event_time = columns[0]->getUInt(row_num);
        EventIdType event_id = columns[1]->getUInt(row_num);

        // Do not deal with unrelated events
        if (event_id == 0) return;

        EventType event_type = getEventType(event_id, events_flag);
        Vector<AttrType> attr_values = getAttrValues(event_id, event_type, columns, row_num);

        TargetValueType target_value = (event_type == EventType::TARGET_EVENT) ? columns[2]->getFloat64(row_num) : 0;

        this->data(place).add(event_type, event_time, event_id, attr_values, target_value, arena);
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

    MultipleEvents getMultipleEvents(Events & events, ResultMap & results) const
    {
        MultipleEvents multiple_events;
        auto start = events.begin(), end = events.end();
        for (auto it = start; it != end; ++it)
        {
            if (it->event_type == EventType::TOUCH_EVENT)
            {
                TouchEvent touch_event{it->event_id, it->attr_values};
                results[touch_event].total_click++;
            }
            else if (it->event_type == EventType::TARGET_EVENT)
            {
                auto next = (std::next(it) == end) ? end : std::next(it);
                multiple_events.emplace_back(std::make_move_iterator(start), std::make_move_iterator(next));
                start = next;
            }
        }
        return multiple_events;
    }

    bool valueAssociation(const Event & target, const Event & procedure) const
    {
        /// No attr association
        if (attr_relation_matrix.empty()) return true;

        size_t index = getProcedureNum(procedure.event_id, events_flag);
        return procedure.attr_values[0] == target.attr_values[index-1];
    }

    inline bool fullProcedure(const std::unordered_set<EventIdType> & valid_procedures) const
    {
        return valid_procedures.size() == (events_flag[1]-events_flag[0]);
    }

    void calculateContribution(Events & valid_touch_events, Event target_event, ResultMap & results) const
    {
        if (unlikely(valid_touch_events.empty())) return;
        size_t size = valid_touch_events.size();
        auto target_value = target_event.target_value;

        // First Touch Attribution
        if (mode_flag & (1))
        {
            const auto & touch_event = valid_touch_events[size-1];
            results[{touch_event.event_id, touch_event.attr_values}].value[0] += target_value;
        }
        // Last Touch Attribution
        if (mode_flag & (1<<1))
        {
            const auto & touch_event = valid_touch_events[0];
            results[{touch_event.event_id, touch_event.attr_values}].value[1] += target_value;
        }
        // Linear Attribution
        if (mode_flag & (1<<2))
        {
            for (const auto & touch_event : valid_touch_events)
                results[{touch_event.event_id, touch_event.attr_values}].value[2] += (target_value*1.0)/size;
        }
        // Location Based Attribution
        if (mode_flag & (1<<3))
        {
            if (size < 3)
            {
                for (const auto & touch_event : valid_touch_events)
                results[{touch_event.event_id, touch_event.attr_values}].value[3] += (target_value*1.0)/size;
            }
            else 
            {
                double avg = p / (size-2)*1.0, extra;
                size_t cnt = 0;
                for (const auto & event : valid_touch_events)
                {
                    extra = (cnt == 0) ? q-avg : (cnt == size-1) ? o-avg : 0.0;
                    results[{event.event_id, event.attr_values}].value[3] += (avg+extra)*target_value;
                }
            }
        }
        // Time Decay Attribution
        if (mode_flag & (1<<4))
        {
            double total = 0.0;
            for (auto & event : valid_touch_events)
            {
                double cur = pow(0.5, (target_event.time-event.time)/t);
                // Temporarily store the intermediate results 
                event.target_value += cur;
                total += cur;
            }
            if (total == 0) return;
            for (const auto & event : valid_touch_events)
                results[{event.event_id, event.attr_values}].value[4] += ((event.target_value/total)*target_value);
        }
    }

    void checkAndCalcValidEvents(Events & events, ResultMap & results) const
    {
        Event & target_event = events[events.size()-1];
        if (unlikely(target_event.event_type != EventType::TARGET_EVENT))
            return;

        Events valid_events;
        bool all_procedure = (events_flag[1] == events_flag[0]);
        std::unordered_set<EventIdType> valid_procedures;
    
        for (int i = events.size()-2; i >= 0; i--)
        {
            Event & event = events[i];

            if (event.event_type == EventType::PROCEDURE_EVENT && !all_procedure)
            {
                if (valueAssociation(target_event, event))
                    valid_procedures.emplace(event.event_id);
                if (fullProcedure(valid_procedures))
                    all_procedure = true;
            }
            else if (event.event_type == EventType::TOUCH_EVENT)
            {
                if (!all_procedure) continue;

                bool out_of_window = window ? (target_event.time - event.time > TimeType(window)) :
                        (date_lut.toDayNum(target_event.time/1000) > date_lut.toDayNum(event.time/1000));
                if (out_of_window)
                    break;

                AnalysisResult & result = results[{event.event_id, event.attr_values}];
                result.valid_click++;
                result.transform_times.push_back(target_event.time-event.time);
                result.transform_steps.push_back(events.size()-1-i);

                valid_events.push_back(std::move(event));
            }
        }

        if (valid_events.empty())
        {
            if (other_transform)
            {
                TouchEvent other_transform_event{0, {}};
                for (size_t i = 0; i < 5; ++i)
                    if (mode_flag & (1<<i))
                        results[other_transform_event].value[i] += target_event.target_value;
                results[other_transform_event].total_click++;
            }
            return;
        }

        calculateContribution(valid_events, target_event, results);
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

    void insertResultIntoColumn(IColumn &to, const ResultMap & results) const
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        ColumnTuple & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());

        // touch_event_to: tuple(UInt16, Array<TYPE>)
        ColumnTuple& touch_event_tuple_to = assert_cast<ColumnTuple &>(tuple_to.getColumn(0));
        auto & event_id_to = assert_cast<ColumnUInt16 &>(touch_event_tuple_to.getColumn(0)).getData();
        ColumnArray& attr_values_to = assert_cast<ColumnArray &>(touch_event_tuple_to.getColumn(1));

        auto & click_cnt_to = assert_cast<ColumnUInt64 &>(tuple_to.getColumn(1)).getData();
        auto & valid_transform_cnt_to = assert_cast<ColumnUInt64 &>(tuple_to.getColumn(2)).getData();
        ColumnArray& transform_times_to = assert_cast<ColumnArray &>(tuple_to.getColumn(3));
        ColumnArray& transform_steps_to = assert_cast<ColumnArray &>(tuple_to.getColumn(4));
        ColumnArray& value_to = assert_cast<ColumnArray &>(tuple_to.getColumn(5));

        for (const auto & result : results)
        {
            event_id_to.push_back(result.first.first);
            if constexpr (std::is_same_v<AttrType, String>)
                insertVectorIntoColumn<ColumnString>(attr_values_to, result.first.second);
            else
                insertVectorIntoColumn<ColumnUInt64>(attr_values_to, result.first.second);

            click_cnt_to.push_back(result.second.total_click);
            valid_transform_cnt_to.push_back(result.second.valid_click);

            insertVectorIntoColumn<ColumnUInt64>(transform_times_to, result.second.transform_times);
            insertVectorIntoColumn<ColumnUInt64>(transform_steps_to, result.second.transform_steps);
            insertVectorIntoColumn<ColumnFloat64>(value_to, result.second.value);
        }
        offsets_to.push_back((offsets_to.empty() ? 0 : offsets_to.back()) + results.size());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        Events & all_events = this->data(place).events;
        std::sort(all_events.begin(), all_events.end());

        ResultMap results;
        MultipleEvents multiple_events = getMultipleEvents(all_events, results);

        for (auto & events : multiple_events)
            checkAndCalcValidEvents(events, results);

        insertResultIntoColumn(to, results);
    }

    bool allocatesMemoryInArena() const override { return false; }

};

}
