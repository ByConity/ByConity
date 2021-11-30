#pragma once

#include <unordered_set>
#include <numeric>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/ArenaAllocator.h>
#include <Common/Stopwatch.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunnelCommon.h>

namespace DB
{

/*
 * The main loop for funnel calculate:
 * | A -> B -> C | will calculate once
 *
 */

template<typename ParamType, typename Numeric, bool is_step, bool with_time, bool with_attr>
void calculateFunnel(std::function<UInt32(UInt32)> && getGroup, EventLists<ParamType, Numeric> & event_lists,
                        LEVELType * levels, std::vector<Times>& intervals,
                        bool is_relative_window, UInt64 m_window, size_t m_numEvents, UInt32 m_watchStep,
                        [[maybe_unused]] UInt32 attrRelated, UInt32 m_watchNumbers, UInt32 m_userProIdx, const DateLUTImpl & dateLut,
                        size_t numSubGroup, bool has_null, [[maybe_unused]] size_t start_step_num = 0,
                        [[maybe_unused]] size_t end_step_num = 0, [[maybe_unused]] bool last_step = true)
{
    auto & events = event_lists;
    auto num_events = events.size();

    if (num_events == 0) return;

    // relative means the window is not a fixed interval such as a day or a week, etc.
    // But the same day, or the same week.
    UInt64 funnel_window = m_window;
    // time interval
    Times cur_times;
    [[maybe_unused]] ssize_t truncate_index = -1;

    size_t i = 0;
    std::vector<std::vector<size_t>> funnel_index;
    std::vector<size_t> s1, s2;
    funnel_index.reserve(2);
    s1.reserve(m_numEvents);
    s2.reserve(m_numEvents);

    funnel_index.emplace_back(s1);
    funnel_index.emplace_back(s2);

    // for groups
    // Group is aligned like this  [ordinary][unreach][null]
    const size_t num_ordinary_group = numSubGroup - 1 - has_null;
    std::vector<bool> reached_flag_for_each_slot(m_watchNumbers + 1, false); // need reset unreach

    // funnel group need count several funnel in a single window
    auto size_base_compare = [](const std::vector<size_t>& x, const std::vector<size_t>& y)
    {
        return x.size() < y.size();
    };
    std::priority_queue<std::vector<size_t>, std::vector<std::vector<size_t>>, decltype(size_base_compare)> funnels(size_base_compare);

    while (true)
    {
        size_t next_seq = 0;
        UInt32 slot_begin = 0, slot_end = 0, slot_idx = 0; // count base by slot
        UInt64 window_start = 0;
        UInt64 window_end = 0;
        int last_start = -1; // for slot smaller than window, need recheck within window start event
        // attribute related
        [[maybe_unused]] ParamType attr_check[2] = {};
        [[maybe_unused]] bool      attr_set[2] = {false, false};
        UInt64 start_window[2] = {0ULL,0ULL};
        funnels.push({}); // sentinel

        if (i < num_events)
            slot_idx = events[i].stime / m_watchStep;

        while (i <= num_events)
        {
            if (unlikely(i == num_events)) // finish loop
            {
                if (last_start == -1) ++i; // no start event in new slot
                size_t max_arr = funnel_index[1].size() < funnel_index[0].size() ? 0 : 1;
                if (funnel_index[max_arr].size() >= funnels.top().size())
                {
                    if (funnels.top().size() < funnel_index[max_arr].size())
                        funnels.pop();
                    funnels.push(funnel_index[max_arr]);
                }

                break;
            }

            auto stime = events[i].stime;
            auto ctime = events[i].ctime;
            auto event = events[i].event;

            // found best funnel
            if (funnel_index[next_seq].size() == m_numEvents)
            {
                if (funnel_index[next_seq].size() >= funnels.top().size())
                {
                    if (funnels.top().size() < funnel_index[next_seq].size())
                        funnels.pop();
                    funnels.push(funnel_index[next_seq]);
                }
                funnel_index[next_seq].resize(0);
            }

            // check valid window
            if (window_start && (ctime > window_end))
            {
                // 1. record the current max funnel
                size_t max_arr = funnel_index[1].size() < funnel_index[0].size() ? 0 : 1;
                if (funnel_index[max_arr].size() >= funnels.top().size())
                {
                    if (funnels.top().size() < funnel_index[max_arr].size())
                        funnels.pop();
                    funnels.push(funnel_index[max_arr]);
                }

                // for group check the other one
                if (funnel_index[!max_arr].size() >= funnels.top().size())
                {
                    if (funnels.top().size() < funnel_index[!max_arr].size())
                        funnels.pop();
                    funnels.push(funnel_index[!max_arr]);
                }

                // 2. drop the outside window seq
                size_t drop = start_window[0] > start_window[1] ? 1 : 0;

                // still in the same slot goto next
                size_t op = !drop;
                bool has_second_chance = false;
                if (start_window[op])
                {
                    slot_begin = slot_idx * m_watchStep;
                    if (is_relative_window)
                    {
                        funnel_window = setValidWindow(start_window[op], dateLut);
                        slot_end = slot_begin + funnel_window/1000 + 1; // exclusive
                    }
                    else
                        slot_end = slot_begin + m_watchStep; // exclusive

                    has_second_chance = (ctime <= (start_window[op] + funnel_window));
                }

                if ((stime >= slot_begin && stime < slot_end) || has_second_chance)
                {
                    funnel_index[drop].resize(0);
                    window_start = 0; // new window

                    // fix
                    if (has_second_chance)
                    {
                        window_start = start_window[op];
                        window_end   = window_start + funnel_window;
                        next_seq = op;
                    }
                    else
                    {
                        funnel_index[op].resize(0);
                        if constexpr (with_attr)
                        {
                            attr_check[op] = {};
                            attr_set[op] = false;
                        }
                    }

                    if constexpr (with_attr)
                    {
                        attr_check[drop] = {};
                        attr_set[drop] = false;
                    }
                }
                else
                    break;
            }

            if (event & 0x1ULL) //new start event
            {
                if constexpr (is_step)
                {
                    if (slot_idx < start_step_num || slot_idx >= end_step_num)
                    {
                        if (truncate_index == -1 && slot_idx >= end_step_num)
                            truncate_index = i;

                        ++i;
                        continue;
                    }
                }
                else
                {
                    if (slot_idx >= m_watchNumbers)
                    {
                        ++i;
                        continue;
                    }
                }

                // the start event must be in the same slot
                if ((stime / m_watchStep) == slot_idx || (event > 1 && !funnel_index[next_seq].empty()))
                {
                    //funnel for same event
                    if (event > 1 && !funnel_index[next_seq].empty() && isNextLevel(event, funnel_index[next_seq].size()))
                    {
                        bool is_legal = true;
                        if constexpr (with_attr)
                        {
                            if (event & attrRelated)
                            {
                                if (attr_set[next_seq])
                                {
                                    if (attr_check[next_seq] != events[i].param) // attr not match
                                    {
                                        is_legal = false;
                                    }
                                }
                                else
                                {
                                    attr_check[next_seq] = events[i].param;
                                    attr_set[next_seq] = true;
                                }
                            }
                        }

                        if (is_legal)
                        {
                            if (funnel_index[next_seq].size() == 1)
                            {
                                if (last_start == -1) last_start = i;
                            }

                            funnel_index[next_seq].push_back(i);
                        }
                        ++i;
                        continue;
                    }
                    else if (event > 1 && !funnel_index[next_seq].empty())
                    {
                        // for A->A->A ..., only use one seq
                        ++i;
                        continue;
                    }

                    if constexpr (with_attr)
                    {
                        if (!funnel_index[0].empty() && !funnel_index[1].empty())
                        {
                            // may have different attr from start event
                            if (last_start == -1) last_start = i;
                            ++i;
                            continue;
                        }
                    }

                    next_seq = funnel_index[0].size() > funnel_index[1].size() ? 1 : 0;
                    bool need_update_window = false;
                    if (funnel_index[next_seq].size() > 1)
                    {
                        if (funnel_index[0].size() != funnel_index[1].size())
                        {
                            if (last_start == -1) last_start = i;
                            ++i;
                            continue;
                        }
                        else
                        {
                            next_seq = start_window[0] > start_window[1] ? 1 : 0;
                            need_update_window = true;
                        }
                    }
                    else if (funnel_index[next_seq].size() == 1 && funnel_index[!next_seq].size() == 1)
                    {
                        next_seq = start_window[0] > start_window[1] ? 1 : 0; // new start replace the old one
                        need_update_window = true;
                    }

                    funnel_index[next_seq].resize(0);
                    funnel_index[next_seq].push_back(i);
                    start_window[next_seq] = ctime;

                    if (window_start == 0 || need_update_window)
                    {
                        window_start = start_window[!next_seq] ? start_window[!next_seq] : ctime;
                        slot_begin = slot_idx * m_watchStep;

                        if (is_relative_window)
                        {
                            funnel_window = setValidWindow(window_start, dateLut);
                            slot_end = slot_begin + funnel_window/1000 + 1; // exclusive
                        }
                        else
                            slot_end = slot_begin + m_watchStep; // exclusive

                        window_end = window_start + funnel_window;
                    }
                    else if (window_start != start_window[0] && window_start != start_window[1])
                    {
                        window_start = start_window[0] >  start_window[1] ? start_window[1] : start_window[0];
                        slot_begin = slot_idx * m_watchStep;

                        if (is_relative_window)
                        {
                            funnel_window = setValidWindow(window_start, dateLut);
                            slot_end = slot_begin + funnel_window/1000 + 1; // exclusive
                        }
                        else
                            slot_end = slot_begin + m_watchStep; // exclusive

                        window_end = window_start + funnel_window;
                    }

                    if constexpr (with_attr)
                    {
                        if (event & attrRelated)
                        {
                            attr_check[next_seq] = events[i].param;
                            attr_set[next_seq] = true;
                        }
                    }
                }
                else
                {
                    if (last_start == -1 && ((stime / m_watchStep) > slot_idx))
                        last_start = i;
                }
            }
            else if (isNextLevel(event, funnel_index[next_seq].size()))
            {
                if constexpr (with_attr)
                {
                    if (event & attrRelated)
                    {
                        if (attr_set[next_seq])
                        {
                            if (attr_check[next_seq] != events[i].param) // attr not match
                            {
                                // check the other seq with attr
                                if (isNextLevel(event, funnel_index[!next_seq].size()))
                                {
                                    if (attr_set[!next_seq] && attr_check[!next_seq] == events[i].param)
                                    {
                                        funnel_index[!next_seq].push_back(i);
                                    }
                                }
                                ++i;
                                continue;
                            }
                        }
                        else
                        {
                            attr_check[next_seq] = events[i].param;
                            attr_set[next_seq] = true;
                        }
                    }
                }

                funnel_index[next_seq].push_back(i);
                // same middel event (flagEvent & (flagEvent - 1))
                if ((event & (event - 1)) && isNextLevel(event, funnel_index[!next_seq].size()))
                {
                    // this event can be reuse in the other seq
                    if constexpr (with_attr)
                    {
                        if (event & attrRelated)
                        {
                            if (attr_set[!next_seq])
                            {
                                if (attr_check[!next_seq] != events[i].param) // attr not match
                                {
                                    ++i;
                                    continue;
                                }
                            }
                            else
                            {
                                attr_check[!next_seq] = events[i].param;
                                attr_set[!next_seq] = true;
                            }
                        }
                    }

                    funnel_index[!next_seq].push_back(i);
                }
            }
            else if (isNextLevel(event, funnel_index[!next_seq].size()))
            {
                if constexpr (with_attr)
                {
                    if (event & attrRelated)
                    {
                        if (attr_set[!next_seq])
                        {
                            if (attr_check[!next_seq] != events[i].param) // attr not match
                            {
                                ++i;
                                continue;
                            }
                        }
                        else
                        {
                            attr_check[!next_seq] = events[i].param;
                            attr_set[!next_seq] = true;
                        }
                    }
                }

                funnel_index[!next_seq].push_back(i);
            }
            ++i;
        }

        // one slot end, count funnel
        while (!funnels.empty())
        {
            const auto & current_window_funnel = funnels.top();
            auto funnel  = current_window_funnel.size();
            if (funnel > 0)
            {
                Numeric group{};
                bool is_null = true;
                for (size_t m = 0; m < current_window_funnel.size(); ++m)
                {
                    if (m_userProIdx == (m + 1))
                    {
                        group = events[current_window_funnel[m]].pro_ind;
                        is_null = events[current_window_funnel[m]].is_null;
                        break;
                    }
                }

                size_t ind;
                if (is_null)
                {
                    ind = num_ordinary_group * (m_watchNumbers + 1) + slot_idx + 1; // unreach
                }
                else
                {
                    ind = getGroup(group) * (m_watchNumbers + 1) + slot_idx + 1;
                    reached_flag_for_each_slot[slot_idx] = true;
                }

                // record this funnel
                if (levels[ind] < funnel)
                {
                    levels[ind] = funnel;
                    if constexpr (with_time)
                    {
                        // time interval
                        Times cur;
                        for (const auto & index : current_window_funnel)
                            cur.push_back(events[index].ctime);

                        intervals[ind] = std::move(cur);
                    }
                }
            }
            funnels.pop();
        }

        // skip to next slot index
        if (!is_relative_window && last_start == -1)
        {
            while (i < num_events)
            {
                if ((events[i].stime / m_watchStep) <= slot_idx)
                    ++i;
                else
                    break;
            }
        }

        // start new round
        i = last_start != -1 ? last_start : i;
        if (i >= num_events)
            break;

        funnel_index[0].resize(0);
        funnel_index[1].resize(0);
    }

    if constexpr (is_step)
    {
        if (truncate_index < 0) truncate_index = num_events;
        event_lists.erase(event_lists.begin(), event_lists.begin() + truncate_index);
    }

    /// reset unreached
    for (size_t m = 0, base = num_ordinary_group * (m_watchNumbers + 1) + 1; m < m_watchNumbers; ++m)
    {
        if (reached_flag_for_each_slot[m])
        {
            levels[base + m] = 0;
            if constexpr (with_time)
                intervals[base + m] = {};
        }
    }

    for (size_t m = 0; m < numSubGroup; m++ )
    {
        auto offset = m * (m_watchNumbers + 1);
        size_t ml = 0;
        for (size_t j = 1 ; j <= m_watchNumbers; j++)
            if (ml < levels[offset + j]) ml = levels[offset + j];

        levels[offset] = ml;
    }
}

/**
 * for numeric group key
 */
template <typename Numeric, typename ParamType>
class AggregateFunctionFinderGroupNumFunnel final :
        public IAggregateFunctionDataHelper<AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric>,
        AggregateFunctionFinderGroupNumFunnel<Numeric, ParamType>>
{
private:
    // Parameters got from agg function
    UInt64 m_watch_start; // start timestamp of 'per day' check
    UInt32 m_watch_step; // granularity of funnel count, e.g. per day
    UInt32 m_watch_numbers; // how many fine granularity checks. e.g. 30 for per day check for one month records
    UInt64 m_window; // only events in the window will be counted as conversion
    UInt32 m_user_pro_idx; // group id index
    Int32 m_window_type; // funnel in the same day, or same week
    String time_zone;  // time_zone helps calculate the datetime a timestamp means for different area.
    DataTypePtr m_user_pro_type; // group type
    size_t m_num_events;
    const DateLUTImpl & date_lut;

    UInt32 attr_related;
    UInt32 related_num;

    bool time_interval = false;
    mutable bool is_step = false;

public:
    AggregateFunctionFinderGroupNumFunnel(UInt64 window, UInt64 watchStart, UInt64 watchStep,
    UInt64 watchNumbers, UInt32 userProIdx, UInt64 window_type, String time_zone_,
    UInt64 numVirts, UInt32 attr_related_, bool time_interval_,
    const DataTypes & arguments, const Array & params) :
    IAggregateFunctionDataHelper<AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric>,
            AggregateFunctionFinderGroupNumFunnel<Numeric, ParamType> >(arguments, params),
    m_watch_start(watchStart), m_watch_step(watchStep), m_watch_numbers(watchNumbers),
    m_window(window), m_user_pro_idx(userProIdx), m_window_type(window_type), time_zone(time_zone_), m_num_events(numVirts),
    date_lut(DateLUT::instance(time_zone_)), attr_related(attr_related_), time_interval(time_interval_)
    {
        related_num = attr_related ? __builtin_popcount(attr_related) + 3 : 3;
        m_user_pro_type = arguments[2];
    }

    String getName() const override
    {
        return "finderGroupFunnel";
    }

//    bool handleNullItSelf() override
//    {
//        return true;
//    }

    void create(const AggregateDataPtr place) const override
    {
        new (place) AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric>;
    }

    DataTypePtr getReturnType() const override
    {
        // Return type is array of tuple type which looks like:
        // [(_, [...]), (_, [...]), ...]
        DataTypes types;
        types.emplace_back(m_user_pro_type);
        types.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<LEVELType> >()));

        if (time_interval)
            types.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<UInt64>>())));

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        // server_timestamp, client_timestamp, event_flag_1, event_flag_2 .....
        auto s_time = static_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
        auto c_time = static_cast<const ColumnVector<UInt64> *>(columns[1])->getData()[row_num];

        // Ignore those columns which start earlier than watch point
        if (unlikely(s_time < m_watch_start))
            return;

        UInt32 stime = static_cast<UInt32>(s_time - m_watch_start);

        UInt32 flag_event = 0;
        int event_index_offset = attr_related ? related_num : 3; // start offset of event flag column, need skip the attr column
        for(size_t i = 0; i < m_num_events; i++)
        {
            UInt32 flag = (static_cast<const ColumnVector<UInt8> *>(columns[i + event_index_offset])->getData()[row_num] != 0);
            flag_event |= (flag << i);
        }

        if (unlikely(flag_event == 0)) return; // Mostly filter ensure none empty

        Numeric user_pro{};
        bool is_null = false;
        // nullable user property support
        if (m_user_pro_type->isNullable())
        {
            // get Nullable indicator
            const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[2]);
            if (column->isNullAt(row_num))
                is_null = true;
            else
                user_pro = static_cast<const ColumnVector<Numeric>&>(column->getNestedColumn()).getData()[row_num];
        }
        else
        {
            user_pro = static_cast<const ColumnVector<Numeric>&>(*columns[2]).getData()[row_num];
        }

        if (attr_related & flag_event)
        {
            int index = __builtin_popcount(((flag_event & attr_related) -1) & attr_related);
            // get correspond param column
            ParamType attr = getAttribution<ParamType>(columns, row_num, index + 3);

            this->data(place).add(stime, c_time, flag_event, user_pro, is_null, attr);
        }
        else
        {
            this->data(place).add(stime, c_time, flag_event, user_pro, is_null);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

//    inline bool needCalculateStep(AggregateDataPtr place) const override
//    {
//        return !this->data(place).event_lists.empty();
//    }
//
//    void calculateStepResult(AggregateDataPtr place, size_t start_step_num, size_t end_step_num, bool lastStep, Arena * arena) const override
//    {
//        is_step = true;
//
//        auto& dict_index = this->data(place).groups;
//        size_t num_sub_group = dict_index.size();
//
//        num_sub_group += 2; // for unreach and null group
//        auto & data_levels = this->data(place).levels;
//        auto & data_intervals = this->data(place).intervals;
//        // numSubGroup could grow in different step, and we need to resize step-ed result.
//
//        size_t old_data_levels_size = data_levels.size();
//        size_t old_num_sub_group = old_data_levels_size / (m_watch_numbers + 1);
//        size_t newSize =  (m_watch_numbers + 1) * num_sub_group;
//
//        if (this->data(place).levels.size() != newSize)
//        {
//            data_levels.resize_fill((m_watch_numbers + 1) * num_sub_group, 0, arena);
//
//            // e.g. oldNumSubGroup is 2 and new numSubGroup is 3
//            // i.e. old levels alignment is:
//            // [g1 .... ] [ g2 ....] [unreach ....] [NULL ....]
//            //
//            // if we need build numSubGroup(3) level, we need to permute existing levels
//            // [g1 .... ] [g2 ....] [g3 ....] [unreach ....] [NULL ...]
//            if (old_num_sub_group >= 2)
//            {
//                // move unreach/NULL group
//                size_t adj_size = 2 * (m_watch_numbers + 1);
//
//                for (size_t i = 0; i < adj_size; i++)
//                {
//                    data_levels[newSize - i - 1] = data_levels[old_data_levels_size - i - 1];
//                    data_levels[old_data_levels_size - i - 1] = 0;
//                }
//            }
//        }
//
//        bool is_relative_window = m_window_type ? true : false;
//        bool has_null = this->data(place).has_null;
//        auto &data_ref = const_cast<AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric>&>(this->data(place));
//        data_ref.sort();
//
//        auto getGroup = [&](UInt32 g){ return data_ref.groups[g];};
//
//        if (time_interval)
//        {
//            old_data_levels_size = data_intervals.size();
//            old_num_sub_group = old_data_levels_size / (m_watch_numbers + 1);
//            newSize =  (m_watch_numbers + 1) * num_sub_group;
//            if (data_intervals.size() != newSize)
//            {
//                data_intervals.resize(newSize);
//                if (old_num_sub_group >= 2)
//                {
//                    // move unreach/NULL group
//                    size_t adjSize = 2 * (m_watch_numbers + 1);
//                    for (size_t i = 0; i < adjSize; i++)
//                    {
//                        data_intervals[newSize - i - 1] = data_intervals[old_data_levels_size - i - 1];
//                        data_intervals[old_data_levels_size - i - 1].clear();
//                    }
//                }
//            }
//
//            if (attr_related > 0)
//                calculateFunnel<ParamType, Numeric, true, true, true>(getGroup, data_ref.event_lists, &(this->data(place).levels[0]), this->data(place).intervals,
//                                                                       is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                       attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null,
//                                                                       start_step_num, end_step_num, lastStep);
//            else
//                calculateFunnel<ParamType, Numeric, true, true, false>(getGroup, data_ref.event_lists, &(this->data(place).levels[0]), this->data(place).intervals,
//                                                                        is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                        attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null,
//                                                                        start_step_num, end_step_num, lastStep);
//        }
//        else
//        {
//            if (attr_related > 0)
//                calculateFunnel<ParamType, Numeric, true, false, true>(getGroup, data_ref.event_lists, &(this->data(place).levels[0]), this->data(place).intervals,
//                                                                       is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                       attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null,
//                                                                       start_step_num, end_step_num, lastStep);
//            else
//                calculateFunnel<ParamType, Numeric, true, false, false>(getGroup, data_ref.event_lists, &(this->data(place).levels[0]), this->data(place).intervals,
//                                                                        is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                        attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null,
//                                                                        start_step_num, end_step_num, lastStep);
//        }
//    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        const auto& dict_index = this->data(place).groups;
        size_t num_sub_group = dict_index.size();
        bool has_null = this->data(place).has_null;
        // NULL group, and unreach group  will be consider as the last two groups, NULL group
        // is after unreached group
        num_sub_group += (1 + has_null);

        // The result type is [(_, [...]), (_, [...]), ....]
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        auto& offsets_to = arr_to.getOffsets();

        // Record last pos
        size_t prev_offset = (offsets_to.empty() ? 0 : offsets_to.back());
        // Set how many tuples will be insert into result array
        offsets_to.push_back(prev_offset + num_sub_group);

        auto& tuple_arr_to = static_cast<ColumnTuple &>(arr_to.getData());

        auto user_pro_type = m_user_pro_type;
        auto user_pro_column = tuple_arr_to.getColumnPtr(0)->assumeMutable();

        // nullable user property support
        if (user_pro_type->isNullable())
        {
            auto& nullable_user_pro_to = static_cast<ColumnNullable &>(*user_pro_column);
            user_pro_column  = nullable_user_pro_to.getNestedColumnPtr()->assumeMutable();
            auto& nullid_user_pro_to = nullable_user_pro_to.getNullMapData();
            nullid_user_pro_to.insert_nzero(num_sub_group);
            if (has_null) { nullid_user_pro_to.back() = 1; }
        }

        auto& user_pro_data_to = static_cast<ColumnVector<Numeric>&>(*user_pro_column).getData();
        auto prev_pos = user_pro_data_to.size();
        user_pro_data_to.insert_nzero(num_sub_group);

        for (auto& dict_idx : dict_index)
        {
            user_pro_data_to[prev_pos + dict_idx.second] = dict_idx.first;
        }

        //The userPro property for unreached group will set as MAX_VAL of TUSRPRO
        user_pro_data_to[prev_pos + num_sub_group -1 - has_null] = std::numeric_limits<Numeric>::max();

        auto& tuple_funnel_arr_to = static_cast<ColumnArray &>(tuple_arr_to.getColumn(1));
        auto& f_data_to = static_cast<ColumnVector<LEVELType> &>(tuple_funnel_arr_to.getData()).getData();
        ColumnArray::Offsets& f_offsets_to = tuple_funnel_arr_to.getOffsets();
        size_t orig_prev_f_offset =  (f_offsets_to.empty() ? 0 : f_offsets_to.back());
        size_t prev_f_offset = orig_prev_f_offset;

        for(size_t i = 0; i < num_sub_group; i++)
        {
            prev_f_offset += (m_watch_numbers + 1);
            f_offsets_to.push_back(prev_f_offset);
        }
        // allocate the result buffer in one pass
        f_data_to.insert_nzero((m_watch_numbers  + 1) * num_sub_group);
        LEVELType* levels = &(f_data_to[orig_prev_f_offset]);
        auto &data_ref = const_cast<AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric>&>(this->data(place));

        if (is_step)
        {
            const LEVELs & data_levels = data_ref.levels;
            auto min_size = std::min<size_t>(data_levels.size(), (m_watch_numbers  + 1) * num_sub_group);
            std::memcpy(levels, data_levels.data(), min_size * sizeof(LEVELType));

            if (time_interval)
            {
                ColumnArray& intervals_to = static_cast<ColumnArray &>(tuple_arr_to.getColumn(2));
                auto &intervals = data_ref.intervals;
                for (size_t i = 0; i < num_sub_group; i++)
                {
                    auto begin_offset = i * (m_watch_numbers+1);
                    auto end_offset = (i+1) * (m_watch_numbers+1);
                    std::vector<Times> group_intervals(intervals.begin()+begin_offset, intervals.begin()+end_offset);
                    for (Times& times : group_intervals)
                        adjacent_difference(times.begin(), times.end(), times.begin());

                    insertNestedVectorNumberIntoColumn(intervals_to, group_intervals);
                }
            }
        }
        else
        {
            std::vector<Times> intervals;
            bool is_relative_window = m_window_type != 0;
            data_ref.sort();

            auto get_group = [&](UInt32 g){ return data_ref.groups[g];};

            if (time_interval)
            {
                ColumnArray& intervals_to = static_cast<ColumnArray &>(tuple_arr_to.getColumn(2));
                intervals.resize((m_watch_numbers  + 1) * num_sub_group);

                if (attr_related > 0)
                    calculateFunnel<ParamType, Numeric, false, true, true>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                           attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
                else
                    calculateFunnel<ParamType, Numeric, false, true, false>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                            attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);

                for (size_t i = 0; i < num_sub_group; i++)
                {
                    auto begin_offset = i * (m_watch_numbers+1);
                    auto end_offset = (i+1) * (m_watch_numbers+1);
                    std::vector<Times> group_intervals(intervals.begin()+begin_offset, intervals.begin()+end_offset);
                    for (Times& times : group_intervals)
                        adjacent_difference(times.begin(), times.end(), times.begin());
                    insertNestedVectorNumberIntoColumn(intervals_to, group_intervals);
                }
                return;
            }
            else
            {
                if (attr_related > 0)
                    calculateFunnel<ParamType, Numeric, false, false, true>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                            attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
                else
                    calculateFunnel<ParamType, Numeric, false, false, false>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                             attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
            }
        }
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionFinderGroupNumFunnel<Numeric, ParamType> &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }
};

/**
 * for string group key
 */
const static String unreach = "unreach";

template <typename ParamType>
class AggregateFunctionFinderGroupFunnel final :
        public IAggregateFunctionDataHelper<AggregateFunctionFinderFunnelStringGroupData<ParamType>,
        AggregateFunctionFinderGroupFunnel<ParamType> >
{
private:
    // Parameters got from agg function
    UInt64 m_watch_start; // start timestamp of 'per day' check
    UInt32 m_watch_step; // granularity of funnel count, e.g. per day
    UInt32 m_watch_numbers; // how many fine granularity checks. e.g. 30 for per day check for one month records
    UInt64 m_window; // only events in the window will be counted as conversion
    UInt32 m_user_pro_idx; // group id index
    Int32 m_window_type; // funnel in the same day, or same week
    String time_zone;  // time_zone helps calculate the datetime a timestamp means for different area.
    UInt32 m_unreach_str_size; // this is used for FixedString type to avoid truncate error.
    DataTypePtr m_user_pro_type; // group type
    size_t m_num_events;
    const DateLUTImpl & date_lut;

    UInt32 attr_related;
    UInt32 related_num;

    bool time_interval = false;
    mutable bool is_step = false;

public:

    AggregateFunctionFinderGroupFunnel(UInt64 window, UInt64 watchStart, UInt64 watchStep,
                                  UInt64 watchNumbers, UInt32 userProIdx, UInt64 window_type, String time_zone_,
                                  UInt64 numVirts, UInt32 attr_related_, bool time_interval_,
                                  const DataTypes & arguments, const Array & params) :
     IAggregateFunctionDataHelper<AggregateFunctionFinderFunnelStringGroupData<ParamType>,
             AggregateFunctionFinderGroupFunnel<ParamType> >(arguments, params),
     m_watch_start(watchStart), m_watch_step(watchStep), m_watch_numbers(watchNumbers),
     m_window(window), m_user_pro_idx(userProIdx), m_window_type(window_type), time_zone(time_zone_), m_num_events(numVirts),
     date_lut(DateLUT::instance(time_zone_)), attr_related(attr_related_), time_interval(time_interval_)
    {
        related_num = attr_related ? __builtin_popcount(attr_related) + 3 : 3;
        m_user_pro_type = arguments[2];
        DataTypePtr nest_type = m_user_pro_type;

        if (nest_type->isNullable())
            nest_type = static_cast<const DataTypeNullable *>(m_user_pro_type.get())->getNestedType();

        if (typeid_cast<const DataTypeFixedString *>(nest_type.get()))
            m_unreach_str_size = std::min(static_cast<const DataTypeFixedString&>(*nest_type).getN(), unreach.size());
        else
            m_unreach_str_size = unreach.size();
    }

    String getName() const override
    {
        return "finderGroupFunnel";
    }

//    bool handleNullItSelf() override
//    {
//        return true;
//    }

    void create(const AggregateDataPtr place) const override
    {
        new (place) AggregateFunctionFinderFunnelStringGroupData<ParamType>;
    }

    DataTypePtr getReturnType() const override
    {
        // Return type is array of tuple type which looks like:
        // [(_, [...]), (_, [...]), ...]
        DataTypes types;
        types.emplace_back(m_user_pro_type);
        types.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<LEVELType> >()));

        if (time_interval)
            types.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<UInt64>>())));

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *arena) const override
    {
        // server_timestamp, client_timestamp, event_flag_1, event_flag_2 .....
        auto s_time = static_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
        auto c_time = static_cast<const ColumnVector<UInt64> *>(columns[1])->getData()[row_num];

        // Ignore those columns which start earlier than watch point
        if (unlikely(s_time < m_watch_start))
            return;

        UInt32 stime = static_cast<UInt32>(s_time - m_watch_start);

        UInt32 flag_event = 0;
        int event_index_offset = attr_related ? related_num : 3; // start offset of event flag column, need skip the attr column
        for(size_t i = 0; i < m_num_events; i++)
        {
            UInt32 flag = (static_cast<const ColumnVector<UInt8> *>(columns[i + event_index_offset])->getData()[row_num] != 0);
            flag_event |= (flag << i);
        }

        if (unlikely(flag_event == 0)) return; // Mostly filter ensure none empty

        StringRef user_pro{};
        bool is_null = false;
        // nullable user property support
        if (m_user_pro_type->isNullable())
        {
            // get Nullable indicator
            const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[2]);
            if (column->isNullAt(row_num))
                is_null = true;
            else
                user_pro = (column->getNestedColumn()).getDataAt(row_num);
        }
        else
        {
            user_pro = columns[2]->getDataAt(row_num);
        }

        if (attr_related & flag_event)
        {
            int index = __builtin_popcount(((flag_event & attr_related) -1) & attr_related);
            // get correspond param column
            ParamType attr = getAttribution<ParamType>(columns, row_num, index + 3);

            this->data(place).add(stime, c_time, flag_event, user_pro, is_null, attr, arena);
        }
        else
        {
            this->data(place).add(stime, c_time, flag_event, user_pro, is_null, arena);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

//    inline bool needCalculateStep(AggregateDataPtr place) const override
//    {
//        return !this->data(place).event_lists.empty();
//    }
//
//    void calculateStepResult(AggregateDataPtr place, size_t start_step_num, size_t end_step_num, bool lastStep, Arena * arena) const override
//    {
//        is_step = true;
//
//        auto& dict_index = this->data(place).dict_index;
//        size_t numSubGroup = dict_index.size();
//
//        numSubGroup += 2; // for unreach and null group
//        auto & data_levels = this->data(place).levels;
//        auto & data_intervals = this->data(place).intervals;
//        // numSubGroup could grow in different step, and we need to resize step-ed result.
//
//        size_t old_data_levels_size = data_levels.size();
//        size_t oldNumSubGroup = old_data_levels_size / (m_watch_numbers + 1);
//        size_t new_size =  (m_watch_numbers + 1) * numSubGroup;
//
//        if (this->data(place).levels.size() != new_size)
//        {
//            data_levels.resize_fill((m_watch_numbers + 1) * numSubGroup, 0, arena);
//
//            // e.g. oldNumSubGroup is 2 and new numSubGroup is 3
//            // i.e. old levels alignment is:
//            // [g1 .... ] [ g2 ....] [unreach ....] [NULL ....]
//            //
//            // if we need build numSubGroup(3) level, we need to permute existing levels
//            // [g1 .... ] [g2 ....] [g3 ....] [unreach ....] [NULL ...]
//            if (oldNumSubGroup >= 2)
//            {
//                // move unreach/NULL group
//                size_t adj_size = 2 * (m_watch_numbers + 1);
//
//                for (size_t i = 0; i < adj_size; i++)
//                {
//                    data_levels[new_size - i - 1] = data_levels[old_data_levels_size - i - 1];
//                    data_levels[old_data_levels_size - i - 1] = 0;
//                }
//            }
//        }
//
//        bool is_relative_window = m_window_type ? true : false;
//        bool has_null = this->data(place).has_null;
//        auto &data_ref = const_cast<AggregateFunctionFinderFunnelStringGroupData<ParamType>&>(this->data(place));
//        data_ref.sort();
//
//        auto get_group = [&](UInt32 g){ return g;};
//
//        if (time_interval)
//        {
//            old_data_levels_size = data_intervals.size();
//            oldNumSubGroup = old_data_levels_size / (m_watch_numbers + 1);
//            new_size =  (m_watch_numbers + 1) * numSubGroup;
//            if (data_intervals.size() != new_size)
//            {
//                data_intervals.resize(new_size);
//                if (oldNumSubGroup >= 2)
//                {
//                    // move unreach/NULL group
//                    size_t adj_size = 2 * (m_watch_numbers + 1);
//                    for (size_t i = 0; i < adj_size; i++)
//                    {
//                        data_intervals[new_size - i - 1] = data_intervals[old_data_levels_size - i - 1];
//                        data_intervals[old_data_levels_size - i - 1].clear();
//                    }
//                }
//            }
//
//            if (attr_related > 0)
//                calculateFunnel<ParamType, Int32, true, true, true>(get_group, data_ref.event_lists, &(data_ref.levels[0]), data_ref.intervals,
//                                                                      is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                      attr_related, m_watch_numbers, m_user_pro_idx, date_lut, numSubGroup, has_null,
//                                                                      start_step_num, end_step_num, lastStep);
//            else
//                calculateFunnel<ParamType, Int32, true, true, false>(get_group, data_ref.event_lists, &(data_ref.levels[0]), data_ref.intervals,
//                                                                       is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                       attr_related, m_watch_numbers, m_user_pro_idx, date_lut, numSubGroup, has_null,
//                                                                       start_step_num, end_step_num, lastStep);
//        }
//        else
//        {
//            if (attr_related > 0)
//                calculateFunnel<ParamType, Int32, true, false, true>(get_group, data_ref.event_lists, &(data_ref.levels[0]), data_ref.intervals,
//                                                                       is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                       attr_related, m_watch_numbers, m_user_pro_idx, date_lut, numSubGroup, has_null,
//                                                                       start_step_num, end_step_num, lastStep);
//            else
//                calculateFunnel<ParamType, Int32, true, false, false>(get_group, data_ref.event_lists, &(data_ref.levels[0]), data_ref.intervals,
//                                                                        is_relative_window, m_window, m_num_events, m_watch_step,
//                                                                        attr_related, m_watch_numbers, m_user_pro_idx, date_lut, numSubGroup, has_null,
//                                                                        start_step_num, end_step_num, lastStep);
//        }
//    }

    void insertSubGroupKey(ColumnString& subgroupColumn, const StringMapDict<UInt32>& dict_index, bool has_null) const
    {
        size_t num_bytes = 0;
        auto& chars = subgroupColumn.getChars();
        auto& offsets = subgroupColumn.getOffsets();
        for (const auto& dict_ind : dict_index.getRawBuf())
            num_bytes += (dict_ind.first.size + 1); // last 1 byte is for null terminate char

        num_bytes += (m_unreach_str_size + 1 + has_null);

        // use old_size as cursor pos
        size_t old_size = chars.size(); // old start point
        chars.resize(old_size + num_bytes); // resize in one batch
        size_t length;

        const auto& dict_raw_buf = dict_index.getRawBuf();
        size_t num_gen_sub_group = dict_raw_buf.size();
        const auto *it = dict_raw_buf.begin();
        for (size_t i = 0; i < num_gen_sub_group; i++)
        {
            it = dict_raw_buf.begin();
            for(; it != dict_raw_buf.end(); ++it)
            {
                // locate corresponding subgroup info
                if (it->second == i) break;
            }
            if (it == dict_raw_buf.end())
                throw Exception("Program error, subgroup " + toString(i) + " not found", ErrorCodes::LOGICAL_ERROR);

            length = it->first.size;
            memcpy(&chars[old_size], it->first.data, length);
            old_size += length;
            chars[old_size] = 0;
            offsets.push_back(++old_size);
        }
        // Unreached group
        memcpy(&chars[old_size], unreach.data(), m_unreach_str_size);
        old_size += m_unreach_str_size;
        chars[old_size] = 0;
        offsets.push_back(++old_size);
        // for last NULL group
        if (has_null)
        {
            chars[old_size] = 0;
            offsets.push_back(old_size +1);
        }
    }

    void insertSubGroupKey(ColumnFixedString& subgroupColumn, const StringMapDict<UInt32>& dict_index, size_t n, bool has_null) const
    {
        auto& chars = subgroupColumn.getChars();
        size_t num_bytes = (dict_index.size() + 1 + has_null) * n;

        // use numBytes as cursor pos now
        size_t old_size = chars.size(); // old start point
        chars.resize_fill(old_size + num_bytes); // resize in one batch

        const auto& dict_raw_buf = dict_index.getRawBuf();
        size_t num_gen_sub_group = dict_raw_buf.size();
        const auto *it = dict_raw_buf.begin();

        for (size_t i = 0; i < num_gen_sub_group; i++)
        {
            it = dict_raw_buf.begin();
            for(; it != dict_raw_buf.end(); ++it)
            {
                // locate corresponding subgroup info
                if (it->second == i) break;
            }
            if (it == dict_raw_buf.end())
            {
                throw Exception("Program error, subgroup " + toString(i) + " not found", ErrorCodes::LOGICAL_ERROR);
            }
            memcpy(&chars[old_size], it->first.data, it->first.size);
            old_size += n;
        }
        // Unreached group
        memcpy(&chars[old_size], unreach.data(), m_unreach_str_size);
        // for last NULL group, do nothing as it has been set
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * ) const override
    {
        auto& dict_index = this->data(place).dict_index;
        size_t dict_size = dict_index.size();
        size_t num_sub_group = dict_size;

        bool has_null = this->data(place).has_null;
        // NULL group will be consider as the last group
        num_sub_group += (1 + has_null);

        // The result type is [(_, [...]), (_, [...]), ....]
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        // Record last pos
        size_t prev_offset = (offsets_to.empty() ? 0 : offsets_to.back());
        // Set how many tuples will be insert into result array
        offsets_to.push_back(prev_offset + num_sub_group);

        auto& tuple_arr_to = static_cast<ColumnTuple &>(arr_to.getData());

        auto user_pro_type = m_user_pro_type;
        auto user_pro_column = tuple_arr_to.getColumnPtr(0)->assumeMutable();

        if (user_pro_type->isNullable())
        {
            auto& nullable_user_pro_to = static_cast<ColumnNullable &>(*user_pro_column);
            // assign numSubGroup attributes
            user_pro_column  = nullable_user_pro_to.getNestedColumnPtr()->assumeMutable();
            auto& nullid_user_pro_to = nullable_user_pro_to.getNullMapData();
            nullid_user_pro_to.insert_nzero(num_sub_group);
            if (has_null) { nullid_user_pro_to.back() = 1; }
            user_pro_type = static_cast<const DataTypeNullable *>(m_user_pro_type.get())->getNestedType();
        }

        auto &data_ref = const_cast<AggregateFunctionFinderFunnelStringGroupData<ParamType> &>(this->data(place));

        if (typeid_cast<const DataTypeString*>(user_pro_type.get()))
        {
            insertSubGroupKey(static_cast<ColumnString&>(*user_pro_column),
                              data_ref.dict_index,
                              has_null);
        }
        else
        {
            insertSubGroupKey(static_cast<ColumnFixedString&>(*user_pro_column),
                              data_ref.dict_index,
                              typeid_cast<const DataTypeFixedString*>(user_pro_type.get())->getN(),
                              has_null);
        }

        auto& tuple_funnel_arr_to =  static_cast<ColumnArray &>(tuple_arr_to.getColumn(1));
        auto& f_data_to = static_cast<ColumnVector<LEVELType> &>(tuple_funnel_arr_to.getData()).getData();
        ColumnArray::Offsets& f_offsets_to = tuple_funnel_arr_to.getOffsets();
        size_t orig_prev_f_offset =  (f_offsets_to.empty() ? 0 : f_offsets_to.back());
        size_t prev_f_offset = orig_prev_f_offset;

        for(size_t i = 0; i < num_sub_group; i++)
        {
            prev_f_offset += (m_watch_numbers + 1);
            f_offsets_to.push_back(prev_f_offset);
        }

        // allocate the result buffer in one pass
        f_data_to.insert_nzero((m_watch_numbers  + 1) * num_sub_group);
        LEVELType* levels = &(f_data_to[orig_prev_f_offset]);

        if (is_step)
        {
            const LEVELs & data_levels = data_ref.levels;
            auto min_size = std::min<size_t>(data_levels.size(), (m_watch_numbers  + 1) * num_sub_group);
            std::memcpy(levels, data_levels.data(), min_size * sizeof(LEVELType));

            if (time_interval)
            {
                ColumnArray& intervals_to = static_cast<ColumnArray &>(tuple_arr_to.getColumn(2));
                auto &intervals = data_ref.intervals;
                for (size_t i = 0; i < num_sub_group; i++)
                {
                    auto begin_offset = i * (m_watch_numbers+1);
                    auto end_offset = (i+1) * (m_watch_numbers+1);
                    std::vector<Times> group_intervals(intervals.begin()+begin_offset, intervals.begin()+end_offset);
                    for (Times& times : group_intervals)
                        adjacent_difference(times.begin(), times.end(), times.begin());

                    insertNestedVectorNumberIntoColumn(intervals_to, group_intervals);
                }
            }
        }
        else
        {
            data_ref.sort();
            auto get_group = [](UInt32 g) {return g;};

            bool is_relative_window = m_window_type != 0;

            std::vector<Times> intervals;
            if (time_interval)
            {
                ColumnArray& intervals_to = static_cast<ColumnArray &>(tuple_arr_to.getColumn(2));
                intervals.resize((m_watch_numbers  + 1) * num_sub_group);

                if (attr_related > 0)
                    calculateFunnel<ParamType, Int32, false, true, true>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                         attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
                else
                    calculateFunnel<ParamType, Int32, false, true, false>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                          attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);

                for (size_t i = 0; i < num_sub_group; i++)
                {
                    auto begin_offset = i * (m_watch_numbers+1);
                    auto end_offset = (i+1) * (m_watch_numbers+1);
                    std::vector<Times> group_intervals(intervals.begin()+begin_offset, intervals.begin()+end_offset);
                    for (Times& times : group_intervals)
                        adjacent_difference(times.begin(), times.end(), times.begin());
                    insertNestedVectorNumberIntoColumn(intervals_to, group_intervals);
                }
                return;
            }
            else
            {
                if (attr_related > 0)
                    calculateFunnel<ParamType, Int32, false, false, true>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                          attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
                else
                    calculateFunnel<ParamType, Int32, false, false, false>(get_group, data_ref.event_lists, levels, intervals, is_relative_window, m_window, m_num_events, m_watch_step,
                                                                           attr_related, m_watch_numbers, m_user_pro_idx, date_lut, num_sub_group, has_null);
            }
        }
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionFinderGroupFunnel<ParamType> &>(*that).add(place, columns, row_num, arena);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }
};

}
