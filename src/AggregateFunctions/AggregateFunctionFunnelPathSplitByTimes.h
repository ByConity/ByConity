#pragma once

#include <cstddef>
#include <cstdlib>
#include <optional>
#include <utility>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunnelCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

constexpr size_t UNREACHED_LEVEL = SIZE_MAX - 1;

template <bool is_terminating_event = false, bool deduplicate = false>
class AggregateFunctionFunnelPathSplitByTimes final : public IAggregateFunctionDataHelper<
                                                   AggregateFunctionFunnelPathSplitData,
                                                   AggregateFunctionFunnelPathSplitByTimes<is_terminating_event>>
{
private:
    UInt64 window;
    UInt64 max_session_depth;
    UInt64 level_flag;
    std::vector<UInt64> extra_prop_flags;
    UInt64 extra_prop_size;
    // extra_prop_level[extra_prop_index] = level
    // The extra_prop with the subscript extra_prop_index can be added to the path as an attribute node with level = 'level'
    std::vector<size_t> extra_prop_level;

public:

    AggregateFunctionFunnelPathSplitByTimes(
        UInt64 window_,
        UInt64 max_session_depth_,
        UInt64 level_flag_,
        std::vector<UInt64> & extra_prop_flags_,
        const DataTypes & arguments,
        const Array & params)
        : IAggregateFunctionDataHelper<
            AggregateFunctionFunnelPathSplitData,
            AggregateFunctionFunnelPathSplitByTimes<is_terminating_event>>(arguments, params)
        , window(window_)
        , max_session_depth(max_session_depth_)
        , level_flag(level_flag_)
        , extra_prop_flags(extra_prop_flags_)
    {
        extra_prop_size = arguments.size() > 3 ? arguments.size() - 3 : 0;

        extra_prop_level.resize(extra_prop_size);
        size_t level = 1, extra_prop_index = 0;
        for (auto extra_prop_flag : extra_prop_flags_)
        {
            while (extra_prop_flag)
            {
                if (extra_prop_flag & 0x1)
                    extra_prop_level[extra_prop_index++] = level;
                ++level;
                extra_prop_flag >>= 1;
            }
        }
    }

    String getName() const override 
    { 
        String name = "funnelPathSplitByTimes";
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

        // index 3...n: extra_param: String type
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
        size_t i = 0;

        FunnelLevelMap level_map;
        PathBuckets buckets;

        while (i < size)
        {
            auto & event = events[i];
            if (isFirstEvent(event))
            {
                buckets.push_back(PathInfoPro{ .path = {}, .level = 0, .begin_time = event.time });
                addFirstNodeIntoBucket(buckets.size()-1, buckets.back(), event, level_map);
            }
            else if (isCommonEvent(event.index))
                addCommonNodeIntoCurrentPath(buckets, event, level_map);
            else
            {
                if (!tryAddNodeAsPropNode(buckets, event, level_map))
                    if (!tryAddNodeAsLevelNode(buckets, event, level_map))
                        addCommonNodeIntoCurrentPath(buckets, event, level_map);
            }
            ++i;
        }
        
        insertPathsIntoResultColumn(to, buckets);
    }

    inline bool isNextLevel(size_t current_level, Event & event) const
    {
        if (nextLevelNeedPropNode(extra_prop_flags, current_level))
        {
            size_t extra_prop_index = getExtraPropIndex(extra_prop_flags, current_level);
            return !event.extra_prop[extra_prop_index].is_null;
        }

        return event.index == (current_level + 1);
    }

    inline bool isFirstEvent(Event & event) const
    {
        return isNextLevel(0, event);
    }

    void addNodeIntoCurrentPath(PathInfoPro & cur_level_path, EventId event_id, StringRef & prop, Time /* time */, bool level_up = false) const
    {
        auto & path = cur_level_path.path;
        path.push_back({event_id, prop});

        if (level_up)
            ++cur_level_path.level;
    }

    // Assume the key is exist
    inline void eraseFromLevelMap(size_t bucket_id, size_t level, FunnelLevelMap & level_map) const
    {
        auto & set = level_map[level];
        set.erase(bucket_id);
    }

    // If the key is not exist, add a new item
    inline void addIntoLevelMap(size_t bucket_id, size_t level, FunnelLevelMap & level_map) const
    {
        auto & set = level_map[level];
        set.insert(bucket_id);
    }
    
    // Take bucket_id out of level-set and put it into (level+1)-set
    inline void updateFunnelLevelMap(size_t bucket_id, size_t level, FunnelLevelMap & level_map, bool add_into_unreaded_level = false) const
    {
        size_t updated_bucket = add_into_unreaded_level ? UNREACHED_LEVEL : level + 1;

        eraseFromLevelMap(bucket_id, level, level_map);
        addIntoLevelMap(bucket_id, updated_bucket, level_map);
    }

    std::optional<std::set<size_t>> canbeLevelNode(EventIndex event_num, const FunnelLevelMap & level_map) const
    {
        size_t current_level = event_num - 1;
        const auto & it = level_map.find(current_level);

        if (it != level_map.end() && !it->second.empty())
            return it->second;
        return std::nullopt;
    }

    inline bool checkSessionSize(PathInfoPro & cur_level_path) const
    {
        return cur_level_path.path.size() < max_session_depth;
    }

    bool checkSessionWindow(PathInfoPro & cur_level_path, const Event & event) const
    {
        if (unlikely(cur_level_path.path.empty()))
            return true;

        if constexpr (is_terminating_event)
            return (cur_level_path.begin_time - event.time) <= window;
        else
            return (event.time - cur_level_path.begin_time) <= window;
    }

    void addLevelNodeIntoCurrentPath(PathInfoPro & cur_level_path, Event & event, size_t cur_level = 0) const
    {
        if (nextLevelNeedPropNode(extra_prop_flags, cur_level))
        {
            size_t prop_node_id = - (cur_level + 1);
            size_t extra_prop_index = getExtraPropIndex(extra_prop_flags, cur_level);
            addNodeIntoCurrentPath(cur_level_path, prop_node_id, event.extra_prop[extra_prop_index].prop, event.time, true);
        }
        else
        {
            addNodeIntoCurrentPath(cur_level_path, event.index, event.param, event.time, true);
        }
    }

    void addCommonNodeIntoCurrentPath(PathBuckets & buckets, Event & event, FunnelLevelMap & level_map) const
    {
        for (size_t bucket_id = 0; bucket_id < buckets.size(); ++bucket_id)
        {
            auto & bucket = buckets[bucket_id];
            bool reachable_bucket = checkSessionWindow(bucket, event);
            if (reachable_bucket)
                addNodeIntoCurrentPath(bucket, event.index, event.param, event.time);

            reachable_bucket &= checkSessionSize(bucket);
            if (!reachable_bucket)
                updateFunnelLevelMap(bucket_id, bucket.level, level_map, true);
        }
    }

    void addFirstNodeIntoBucket(size_t bucket_id, PathInfoPro & cur_path_info, Event & event, FunnelLevelMap & level_map) const
    {
        addLevelNodeIntoCurrentPath(cur_path_info, event, 0);
        addIntoLevelMap(bucket_id, 1, level_map);
    }

    bool tryUpdateBucket(size_t bucket_id, PathInfoPro & cur_path_info, Event & event, size_t level, FunnelLevelMap & level_map) const
    {
        bool reachable_level = checkSessionWindow(cur_path_info, event);
        if (!reachable_level)
        {
            updateFunnelLevelMap(bucket_id, level, level_map, true);
            return false;
        }

        addLevelNodeIntoCurrentPath(cur_path_info, event, level);
        reachable_level &= checkSessionSize(cur_path_info);
        updateFunnelLevelMap(bucket_id, level, level_map, !reachable_level);
        return true;
    }

    bool tryAddNodeAsPropNode(PathBuckets & buckets, Event & event, FunnelLevelMap & level_map) const
    {
        for (size_t extra_prop_index = 0; extra_prop_index < event.extra_prop.size(); ++extra_prop_index)
        {
            const auto & prop = event.extra_prop[extra_prop_index];
            if (prop.is_null)
                continue;

            size_t next_level = extra_prop_level[extra_prop_index];
            auto bucket_opt = canbeLevelNode(EventIndex(next_level), level_map);
            if (bucket_opt.has_value())
            {
                const auto & bucket_ids = bucket_opt.value();
                size_t level = next_level - 1;
                for (const auto & bucket_id : bucket_ids)
                {
                    if (tryUpdateBucket(bucket_id, buckets[bucket_id], event, level, level_map))
                        return true;
                }
            }
        }
        return false;
    }

    // All noted events can be added as prop node, but only funnel events can be added as level node
    bool tryAddNodeAsLevelNode(PathBuckets & buckets, Event & event, FunnelLevelMap & level_map) const
    {
        if (!isFunnelEvent(level_flag, event.index))
            return false;

        auto bucket_opt = canbeLevelNode(event.index, level_map);
        if (bucket_opt.has_value())
        {
            const auto & bucket_ids = bucket_opt.value();
            size_t level = event.index - 1;
            for (const auto & bucket_id : bucket_ids)
            {
                if (tryUpdateBucket(bucket_id, buckets[bucket_id], event, level, level_map))
                    return true;
            }
        }
        return false;
    }

    // insertMaxPathIntoResultColumn
    void insertPathsIntoResultColumn(IColumn & to, PathBuckets & path_buckets) const
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
        size_t session_num = 0;

        auto insert_session = [&] (PathInfoPro & cur_path_info)
        {
            auto & res_events = cur_path_info.path;
            if (res_events.empty())
                return;

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
            ++session_num;
        };

        for (auto & path_info : path_buckets)
            insert_session(path_info);

        arr_offset.push_back(session_num + (!arr_offset.empty() ? arr_offset.back() : 0));
    }

    bool allocatesMemoryInArena() const override { return true; }

    bool handleNullItSelf() const override { return true; }
};

}
