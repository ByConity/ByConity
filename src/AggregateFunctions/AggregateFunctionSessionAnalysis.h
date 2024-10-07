/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <pdqsort.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>


namespace DB
{
struct AggregateFunctionSessionAnalysisData
{
    using EventType = UInt8;
    using Time = UInt64;
    struct Event
    {
        EventType type;
        Time time;
        StringRef value;
        Event() = default;
        Event(Event && e) : type(e.type), time(e.time), value(e.value) { e.value = StringRef(); }
        Event(EventType _type, Time _time, StringRef _value) : type(_type), time(_time), value(_value) { }
        Event & operator=(Event && e)
        {
            type = e.type;
            time = e.time;
            value = e.value;
            e.value = StringRef();
            return *this;
        }
    };
    using Allocator = MixedArenaAllocator<8192>;
    using Events = PODArray<Event, 32 * sizeof(Event), Allocator>;
    Events events;

    bool sorted = false;
    void sort()
    {
        if (sorted)
            return;
        pdqsort(events.begin(), events.end(), [](Event & lhs, Event & rhs) { return lhs.time < rhs.time; });
        sorted = true;
    }

    void add(EventType type, Time time, StringRef value, Arena * arena) 
    { 
        events.push_back(Event(type, time, value), arena); 
    }

    void merge(const AggregateFunctionSessionAnalysisData & other, Arena * arena)
    {
        sorted = false;
        size_t size = events.size();
        events.insert(std::begin(other.events), std::end(other.events), arena);
        // realloc from arena
        for (size_t i = size; i < events.size(); ++i)
        {
            events[i].value = StringRef(arena->insert(events[i].value.data, events[i].value.size), events[i].value.size);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        size_t size = events.size();
        writeBinary(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            writeBinary(events[i].type, buf);
            writeBinary(events[i].time, buf);
            writeBinary(events[i].value, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        readBinary(sorted, buf);
        size_t size;
        readBinary(size, buf);
        events.reserve(size, arena);
        for (size_t i = 0; i < size; ++i)
        {
            EventType type;
            Time time;
            readBinary(type, buf);
            readBinary(time, buf);
            StringRef value = readStringBinaryInto(*arena, buf);
            add(type, time, value, arena);
        }
    }

    void print() const
    {
        String s = "Event size: " + std::to_string(events.size()) + "\n";
        for (const auto & event : events)
            s += "Event(type=" + std::to_string(event.type) + ", time=" + std::to_string(event.time) + ", value=" + event.value.toString() + ")\n";
        LOG_DEBUG(getLogger("AggregateFunctionSessionAnalysis"), "events:" + s + ".");
    }
};


template <bool has_start_event, bool has_end_event, bool has_target_event, bool with_virtual_event> // for constexpr
class AggregateFunctionSessionAnalysis final
    : public IAggregateFunctionDataHelper<
          AggregateFunctionSessionAnalysisData,
          AggregateFunctionSessionAnalysis<has_start_event, has_end_event, has_target_event, with_virtual_event>>
{
private:
    UInt64 max_session_size;
    // UInt64 windowSize;
    DataTypes types;
    UInt8 argument_size;
    UInt8 start_param_index;
    String start_event;
    String target_event;
    String end_event;
    UInt8 start_bit = 1 << 0;
    UInt8 target_bit = 1 << 1;
    UInt8 end_bit = 1 << 2;
    bool use_first_event_param;

public:
    AggregateFunctionSessionAnalysis(
        UInt64 max_seesion_size_,
        /*UInt64 window_size,*/ String start_event_,
        String end_event_,
        String target_event_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionSessionAnalysisData, AggregateFunctionSessionAnalysis>(arguments, params)
        , max_session_size(max_seesion_size_)
        , /*windowSize(window_size),*/ start_event(start_event_)
        , target_event(target_event_)
        , end_event(end_event_)
    {
        argument_size = arguments.size();
        start_param_index = with_virtual_event ? 3 : 2;

        types.reserve(argument_size - start_param_index + 4);
        types.emplace_back(std::make_shared<DataTypeUInt32>()); // session duration
        types.emplace_back(std::make_shared<DataTypeUInt32>()); // session depth
        types.emplace_back(std::make_shared<DataTypeString>()); // end event
        types.emplace_back(std::make_shared<DataTypeUInt64>()); // session time
        
        for (size_t i = start_param_index; i < argument_size; ++i)
            types.emplace_back(arguments[i]); // param type

        if (start_event == target_event)
        {
            start_bit |= target_bit;
        }
        if (start_event == end_event)
        {
            start_bit |= end_bit;
        }
        if (target_event == end_event)
        {
            target_bit |= end_bit;
        }

        use_first_event_param = params.size() == 4 || params[4].safeGet<UInt64>() == 0;
    }

    String getName() const override 
    {
        if constexpr (with_virtual_event)
            return "vSessionAnalysis";
        else
            return "sessionAnalysis";
    }

    // [(session_duratin, session_depth, end_event, param...)]
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types)); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        //index 0: event
        UInt8 event_type = 0;
        if constexpr (with_virtual_event)
        {
            event_type = columns[2]->getUInt(row_num);
        }
        else if constexpr (has_start_event || has_target_event || has_end_event)
        {
            do
            {
                StringRef t_event = columns[0]->getDataAt(row_num);
                if constexpr (has_start_event)
                {
                    if (t_event == start_event)
                    {
                        event_type = start_bit;
                        break;
                    }
                }
                if constexpr (has_target_event)
                {
                    if (t_event == target_event)
                    {
                        event_type = target_bit;
                        break;
                    }
                }
                if constexpr (has_end_event)
                {
                    if (t_event == end_event)
                    {
                        event_type = end_bit;
                        break;
                    }
                }
            } while (false);
        }

        //index 1: time
        UInt64 time = columns[1]->getUInt(row_num);

        //event,param1,param2,...
        auto seriallize_value = [&]() {
            // alloc from arena
            const char * begin = nullptr;
            size_t value_size = columns[0]->serializeValueIntoArena(row_num, *arena, begin).size; // event

            if constexpr (has_target_event)
            {
                if (!(event_type & (1 << 1)))
                    return StringRef(begin, value_size);
            }

            for (size_t i = start_param_index; i < argument_size; ++i)
                value_size += columns[i]->serializeValueIntoArena(row_num, *arena, begin).size;

            return StringRef(begin,value_size);
        };

        this->data(place).add(event_type, time, seriallize_value(), arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).serialize(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = const_cast<AggregateFunctionSessionAnalysisData &>(this->data(place));
        data.sort();
        auto & events = data.events;
        size_t size = events.size();
        size_t session_num = 0;

        auto & arr_col = static_cast<ColumnArray &>(to);
        auto & arr_offset = arr_col.getOffsets();
        auto & arr_data = static_cast<ColumnTuple &>(arr_col.getData());
        auto & duration_col = static_cast<ColumnUInt32 &>(arr_data.getColumn(0));
        auto & depth_col = static_cast<ColumnUInt32 &>(arr_data.getColumn(1));
        auto & end_event_col = arr_data.getColumn(2);
        auto & session_time_col = static_cast<ColumnUInt64 &>(arr_data.getColumn(3));

        auto insert_session = [&](size_t start, std::vector<size_t> targets, size_t end) 
        {
            // duration
            if constexpr (has_target_event) 
            {
                UInt32 duration = (events[std::min(targets.back() + 1, end)].time - events[targets.back()].time);
                for (size_t i = 0, sentinel = targets.size() - 1; i < sentinel; ++i)
                    duration += (events[targets[i] + 1].time - events[targets[i]].time);
                duration_col.insertValue(duration);
            }
            else 
                duration_col.insertValue(events[end].time - events[start].time);
            
            // depth
            if constexpr (has_target_event)
                depth_col.insertValue(targets.size());
            else
                depth_col.insertValue(end - start + 1);

            // end event
            end_event_col.deserializeAndInsertFromArena(events[end].value.data);
            // session time
            session_time_col.insertValue(events[targets[0]].time);
            // params
            size_t param_event_index = use_first_event_param ? targets[0]: targets.back();
            const auto *pos = events[param_event_index].value.data;
            pos += sizeof(size_t) + unalignedLoad<size_t>(pos); // skip event(string)
            for (size_t i = 0; i < argument_size - start_param_index; ++i)
                pos = arr_data.getColumn(i + 4).deserializeAndInsertFromArena(pos);

            ++session_num;
        };

        size_t i = 0;
        while (i < size)
        {
            size_t start = i, end = size;
            std::vector<size_t> targets;
            // UInt64 window_limit = (events[i].time/windowSize + 1) * windowSize;
            UInt64 session_limit = events[i].time + max_session_size;
            while (i < size)
            {
                //next start
                if constexpr (has_start_event)
                {
                    if (events[i].type & (1 << 0) && i != start)
                    {
                        --i;
                        break;
                    }
                }
                // out of limit
                if (events[i].time >= session_limit)
                {
                    --i;
                    break;
                }
                // if (events[i].time > window_limit) { // >= or >
                //     --i;
                //     break;
                // }
                //target
                if constexpr (has_target_event)
                {
                    if (events[i].type & (1 << 1))
                        targets.emplace_back(i);
                }
                //end
                if constexpr (has_end_event)
                {
                    if (events[i].type & (1 << 2))
                        break;
                }
                session_limit = events[i].time + max_session_size;
                ++i;
            }
            end = i == size ? size - 1 : i;
            ++i;
            if constexpr (has_target_event)
            {
                if (targets.empty())
                    continue;
            }
            else
            {
                targets.emplace_back(start);
                targets.emplace_back(end);
            }
            //process session
            insert_session(start, targets, end);
            //next session
        }
        arr_offset.push_back(session_num + (!arr_offset.empty() ? arr_offset.back() : 0));
    }

    bool allocatesMemoryInArena() const override { return true; }

    bool handleNullItSelf() const override { return true; }

};

}
