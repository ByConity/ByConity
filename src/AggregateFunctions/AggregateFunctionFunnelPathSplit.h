#pragma once

#include <cstdlib>
#include <utility>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunnelCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>


namespace DB
{

template <bool is_terminating_event = false, bool deduplicate = false>
class AggregateFunctionFunnelPathSplit final : public IAggregateFunctionDataHelper<
                                                   AggregateFunctionFunnelPathSplitData,
                                                   AggregateFunctionFunnelPathSplit<is_terminating_event>>
{
private:
    UInt64 window;
    UInt64 max_session_depth;
    UInt64 level_flag;
    std::vector<UInt64> extra_prop_flags;
    UInt64 extra_prop_size;

public:

    AggregateFunctionFunnelPathSplit(
        UInt64 window_,
        UInt64 max_session_depth_,
        UInt64 level_flag_,
        std::vector<UInt64> & extra_prop_flags_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionDataHelper<
            AggregateFunctionFunnelPathSplitData,
            AggregateFunctionFunnelPathSplit<is_terminating_event>>(arguments, params)
        , window(window_)
        , max_session_depth(max_session_depth_)
        , level_flag(level_flag_)
        , extra_prop_flags(std::move(extra_prop_flags_))
    {
        extra_prop_size = arguments.size() > 3 ? arguments.size() - 3 : 0;
    }

    String getName() const override 
    { 
        String name = "funnelPathSplit";
        if constexpr (is_terminating_event)
            name += "R";
        if constexpr (deduplicate)
            name += "D";
        return name; 
    }

    // [[(event,param)...]...]
    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        types.emplace_back(std::make_shared<DataTypeNumber<EventId>>()); // event index
        types.emplace_back(std::make_shared<DataTypeString>()); // param
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types)));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        // index 0: time
        UInt64 time = columns[0]->getUInt(row_num);

        // index 1: event
        EventIndex event_index = columns[1]->getUInt(row_num);

        // index 2...: param
        StringRef t_param = columns[2]->getDataAt(row_num);
        StringRef param = StringRef(arena->insert(t_param.data, t_param.size), t_param.size);

        // index 3...n: extra_param: String / Nullable(String) type
        ExtraPropVec extra_prop;
        for (size_t i = 0; i < extra_prop_size; ++i)
        {
            const auto * column = columns[3+i];
            if (column->isNullable())
            {
                const ColumnNullable * col = static_cast<const ColumnNullable *>(column);
                if (col->isNullAt(row_num))
                {
                    extra_prop.push_back(ExtraProp{true, StringRef("")});
                    continue;
                }
                column = &col->getNestedColumn();
            }

            StringRef prop = column->getDataAt(row_num);
            StringRef prop_n = StringRef(arena->insert(prop.data, prop.size), prop.size);
            extra_prop.push_back(ExtraProp{false, prop_n});
        }

        this->data(place).add(event_index, time, param, extra_prop, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).serialize(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override { this->data(place).deserialize(buf, arena); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        auto & data = const_cast<AggregateFunctionFunnelPathSplitData &>(this->data(place));
        data.sort(is_terminating_event);
        auto & events = data.events;
        size_t size = events.size();

        PathInfo max_level_path;
        PathInfo cur_level_path;

        size_t i = 0;
        size_t level = 0;
        size_t next_first_event = 0;

        auto find_first_event = [&]() -> size_t
        {
            if (next_first_event)
                return next_first_event;

            while (i < size)
            {
                if (isNextLevel(0, events[i]))
                    return i;
                ++i;
            }
            return size;
        };

        while (i < size)
        {
            i = find_first_event();
            if (i >= size)
                break;
            auto & start_event = events[i];
            addLevelNodeIntoCurrentPath(cur_level_path, start_event, 0);

            Time start_time = events[i].time;
            Time duration = 0;
            level = 1;
            next_first_event = 0;

            while (++i < size)
            {
                auto event = events[i];
                if constexpr (is_terminating_event)
                    duration = start_time - event.time;
                else
                    duration = event.time - start_time;

                if (duration > window || cur_level_path.path.size() >= max_session_depth)
                    break;

                if (isFirstEvent(event) && !next_first_event)
                    next_first_event = i;

                if (isCommonEvent(event.index) || !isNextLevel(level, events[i]))
                    addNodeIntoCurrentPath(cur_level_path, event.index, event.param);
                else
                    addLevelNodeIntoCurrentPath(cur_level_path, event, level++);
            }

            if (checkMaxLevel(max_level_path, cur_level_path))
            {
                insertMaxPathIntoResultColumn(to, max_level_path);
                return;
            }

            cur_level_path.clear();
        }

        checkMaxLevel(max_level_path, cur_level_path);
        insertMaxPathIntoResultColumn(to, max_level_path);
    }

    inline bool isNextLevel(size_t current_level, Event & event) const
    {
        if (nextLevelNeedPropNode(extra_prop_flags, current_level))
        {
            size_t extra_prop_index = getExtraPropIndex(extra_prop_flags, current_level);
            return !event.extra_prop[extra_prop_index].is_null;
        }

        return isFunnelEvent(level_flag, event.index) && event.index == (current_level + 1);
    }

    inline bool isFirstEvent(Event & event) const
    {
        return isNextLevel(0, event);
    }

    void addNodeIntoCurrentPath(PathInfo & cur_level_path, EventId event_num, StringRef & prop, bool level_up = false) const
    {
        auto & path = cur_level_path.path;
        path.push_back({event_num, prop});

        if (level_up)
            ++cur_level_path.level;
    }

    void addLevelNodeIntoCurrentPath(PathInfo & cur_level_path, Event & event, size_t cur_level = 0) const
    {
        if (nextLevelNeedPropNode(extra_prop_flags, cur_level))
        {
            size_t prop_node_id = - (cur_level + 1);
            size_t extra_prop_index = getExtraPropIndex(extra_prop_flags, cur_level);
            addNodeIntoCurrentPath(cur_level_path, prop_node_id, event.extra_prop[extra_prop_index].prop, true);
        }
        else
        {
            addNodeIntoCurrentPath(cur_level_path, event.index, event.param, true);
        }
    }

    bool checkMaxLevel(PathInfo & max_level_path, PathInfo & cur_level_path) const
    {
        if (cur_level_path.level > max_level_path.level)
        {
            max_level_path.path = std::move(cur_level_path.path);
            max_level_path.level = cur_level_path.level;
        }

        return max_level_path.level == level_flag;
    }

    void insertMaxPathIntoResultColumn(IColumn & to, PathInfo & max_level_path) const
    {
        /*
        arr_data
            arr_data_data
                arr_data_data.at(0)		event index
                arr_data_data.at(1)		string param
            arr_data_offsets
        arr_offsets
        */
        auto & arr_col = static_cast<ColumnArray &>(to);
        auto & arr_data = static_cast<ColumnArray &>(arr_col.getData());
        auto & arr_offset = arr_col.getOffsets();

        auto & arr_data_data = static_cast<ColumnTuple &>(arr_data.getData());
        auto & arr_data_offset = arr_data.getOffsets();

        auto & event_col = static_cast<ColumnVector<EventId> &>(arr_data_data.getColumn(0));
        auto & param_col = arr_data_data.getColumn(1);

        // When calc count, only return the first max level path
        const size_t session_num = 1;

        auto & res_events = max_level_path.path;
        if (res_events.empty())
        {
            arr_offset.push_back(!arr_offset.empty() ? arr_offset.back() : 0);
            return;
        }

        event_col.insertValue(res_events[0].id);
        param_col.insertData(res_events[0].prop.data, res_events[0].prop.size);
        size_t n = 1;
        for (size_t i = 1; i < res_events.size() && n < max_session_depth; ++i)
        {
            if constexpr (deduplicate)
            {
                if (res_events[i].id == res_events[i - 1].id && res_events[i].prop == res_events[i - 1].prop)
                    continue;
            }
            event_col.insertValue(res_events[i].id);
            param_col.insertData(res_events[i].prop.data, res_events[i].prop.size);
            ++n;
        }
        arr_data_offset.push_back(n + (!arr_data_offset.empty() ? arr_data_offset.back() : 0));

        arr_offset.push_back(session_num + (!arr_offset.empty() ? arr_offset.back() : 0));
    }

    bool allocatesMemoryInArena() const override { return true; }

    bool handleNullItSelf() const override { return true; }

};

}
