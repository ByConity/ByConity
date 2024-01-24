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

#include <AggregateFunctions/AggregateFunnelMapDict.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunnelMapDict.h>
#include <AggregateFunctions/QuantileTDigest.h>

#include <Common/ArenaAllocator.h>
#include <Common/PODArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>

#include <Columns/ColumnsNumber.h>
#include <common/logger_useful.h>

#include "pdqsort.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

using ArithmeticType = Float64;
using IntervalType = UInt64;
struct Arithmetics
{
    ArithmeticType avg_count{};
    ArithmeticType avg_sum{};
    ArithmeticType max = -1;
    ArithmeticType min = INFINITY;
    QuantileTDigest<ArithmeticType> quantileTDigest;
};

using Allocator = MixedArenaAllocator<4096>;
using Times = std::vector<UInt64>;
using LEVELs = PODArray<UInt8, 32*sizeof(UInt8), Allocator>;
using LEVELType = UInt8;

const static int INIT_VECTOR_SIZE = 4; // each user have default event size
const static int NUMBER_STEPS = 64; // support max 64 steps for funnel
const static String unreach = "unreach";

template <typename ParamType>
struct TimeEvent
{
    UInt64 ctime; // Client time
    UInt32 event;
    UInt32 stime; // Mainly server time
    ParamType param; // Event param for funnel compute
    TimeEvent() = default;
    TimeEvent(UInt64 _ctime, UInt32 _event, UInt32 _stime, ParamType _param) : ctime(std::move(_ctime)), event(std::move(_event))
            , stime(std::move(_stime)), param(std::move(_param)) {}
    TimeEvent(UInt64 _ctime, UInt32 _event, UInt32 _stime) : ctime(std::move(_ctime)), event(std::move(_event))
            , stime(std::move(_stime)) {}
};

template <typename ParamType, typename PropType>
struct TimeEventWithProInd
{
    UInt64 ctime; // Client time
    UInt32 event;
    UInt32 stime; // Mainly server time
    PropType pro_ind;
    bool is_null; // used when PropType is numeric
    ParamType param; // Event param for funnel compute

    TimeEventWithProInd() = default;
    TimeEventWithProInd(UInt64 _ctime, UInt32 _event, UInt32 _stime, PropType _prop, bool _is_null, ParamType _param) : ctime(_ctime), event(_event)
            , stime(_stime), pro_ind(_prop), is_null(_is_null), param(std::move(_param)){}
    TimeEventWithProInd(UInt64 _ctime, UInt32 _event, UInt32 _stime, PropType _prop, bool _is_null) : ctime(_ctime), event(_event)
            , stime(_stime), pro_ind(_prop), is_null(_is_null) {}
};

template <typename ParamType>
struct AggregateFunctionFinderFunnelData
{
    using Allocator = MixedArenaAllocator<4096>;
    using EventLists = std::vector<TimeEvent<ParamType>, TrackAllocator<TimeEvent<ParamType>>>;
    EventLists event_lists;
    LEVELs levels;
    std::vector<Times> intervals;
    bool sorted = true;

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag, ParamType &attr)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        event_lists.template emplace_back(ctime, flag, stime, attr);
    }

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        event_lists.template emplace_back(ctime, flag, stime);
    }

    void merge(const AggregateFunctionFinderFunnelData<ParamType> &rhs, Arena *)
    {
        std::copy(std::move_iterator(rhs.event_lists.begin()), std::move_iterator(rhs.event_lists.end()), std::back_inserter(event_lists));
        sorted = false;
    }

    template<bool need_order = false>
    void sort()
    {
        if (!sorted)
        {
            auto compare = [](const TimeEvent<ParamType> &left, const TimeEvent<ParamType> &right) {
                if constexpr (need_order)
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event) || (left.ctime == right.ctime && left.event == right.event && left.stime < right.stime);
                else
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event);
            };

            std::sort(std::begin(event_lists), std::end(event_lists), compare);
            sorted = true;
        }
    }

    void truncate(size_t index)
    {
        event_lists.erase(event_lists.begin(), event_lists.begin() + index);
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = event_lists.size();
        writeBinary(sorted, buf);
        writeBinary(size, buf);
        for (const auto& event : event_lists)
        {
            writeBinary(event.ctime, buf);
            writeBinary(event.event, buf);
            writeBinary(event.stime, buf);
            writeBinary(event.param, buf);
        }

        size = levels.size();
        writeBinary(size, buf);
        if (size > 0)
            buf.write(reinterpret_cast<const char *>(&levels[0]),size * sizeof(levels[0]));

        size = intervals.size();
        writeBinary(size, buf);
        if (size > 0)
        {
            for (const auto& times : intervals)
                writeBinary(times, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena* arena)
    {
        size_t size = 0;
        readBinary(sorted, buf);
        readBinary(size, buf);
        event_lists.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            TimeEvent<ParamType> event;
            readBinary(event.ctime, buf);
            readBinary(event.event, buf);
            readBinary(event.stime, buf);
            readBinary(event.param, buf);

            event_lists.template emplace_back(event);
        }

        readBinary(size, buf);
        if (size > 0)
        {
            levels.resize(size, arena);
            buf.read(reinterpret_cast<char *>(&levels[0]),size * sizeof(levels[0]));
        }

        readBinary(size, buf);
        if (size > 0)
        {
            intervals.reserve(size);
            for (size_t i = 0; i < size; i++)
            {
                Times times;
                readBinary(times, buf);
                intervals.emplace_back(times);
            }
        }
    }
};

template<typename ParamType, typename Numeric>
using EventLists = std::vector<TimeEventWithProInd<ParamType, Numeric>, TrackAllocator<TimeEventWithProInd<ParamType, Numeric>>>;

/**
 * Numeric group data
 */
template <typename ParamType, typename Numeric>
struct AggregateFunctionFinderFunnelNumericGroupData
{
    using Allocator = MixedArenaAllocator<4096>;
    using GroupData = std::unordered_map<Numeric, UInt32>;

    EventLists<ParamType, Numeric> event_lists;
    LEVELs levels;
    std::vector<Times> intervals;
    GroupData  groups;

    bool has_null = false;
    bool sorted = true;

    inline void addProp(Numeric &p)
    {
        size_t index = groups.size();
        groups.emplace(p, index);
    }

    template<bool need_order = false>
    void sort()
    {
        if (!sorted)
        {
            auto compare = [](const TimeEventWithProInd<ParamType, Numeric> &left, const TimeEventWithProInd<ParamType, Numeric> &right) {
                if constexpr (need_order)
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event) || (left.ctime == right.ctime && left.event == right.event && left.stime < right.stime);
                else
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event);
            };

            std::sort(std::begin(event_lists), std::end(event_lists), compare);
            sorted = true;
        }
    }

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag, Numeric &p, bool is_null, ParamType &attr)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (is_null)
            has_null = true;
        else
            addProp(p);

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        event_lists.template emplace_back(ctime, flag, stime, p, is_null, attr);
    }

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag, Numeric& p, bool is_null)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (is_null)
            has_null = true;
        else
            addProp(p);

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        event_lists.template emplace_back(ctime, flag, stime, p, is_null);
    }

    void merge(const AggregateFunctionFinderFunnelNumericGroupData<ParamType, Numeric> &rhs, Arena *)
    {
        for (const auto &it : rhs.groups)
        {
            size_t index = groups.size();
            groups.emplace(it.first, index);
        }

        has_null |= rhs.has_null;
        std::copy(std::move_iterator(rhs.event_lists.begin()), std::move_iterator(rhs.event_lists.end()), std::back_inserter(event_lists));
        sorted = false;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        size_t size = event_lists.size();
        writeBinary(size, buf);
        for (const auto& event : event_lists)
        {
            writeBinary(event.ctime, buf);
            writeBinary(event.event, buf);
            writeBinary(event.stime, buf);
            writeBinary(event.pro_ind, buf);
            writeBinary(event.param, buf);
            writeBinary(event.is_null, buf);
        }

        size = levels.size();
        writeBinary(size, buf);
        if (size > 0)
            buf.write(reinterpret_cast<const char *>(&levels[0]),size * sizeof(levels[0]));

        size = intervals.size();
        writeBinary(size, buf);
        if (size > 0)
        {
            for (const auto& times : intervals)
                writeBinary(times, buf);
        }

        writeBinary(has_null, buf);
        size = groups.size();
        writeBinary(size, buf);
        for (const auto &it : groups)
        {
            writeBinary(it.first, buf);
            writeBinary(it.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena* arena)
    {
        size_t size = 0;
        readBinary(sorted, buf);
        readBinary(size, buf);
        event_lists.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            TimeEventWithProInd<ParamType, Numeric> event;
            readBinary(event.ctime, buf);
            readBinary(event.event, buf);
            readBinary(event.stime, buf);
            readBinary(event.pro_ind, buf);
            readBinary(event.param, buf);
            readBinary(event.is_null, buf);

            event_lists.template emplace_back(event);
        }

        readBinary(size, buf);
        if (size > 0)
        {
            levels.resize(size, arena);
            buf.read(reinterpret_cast<char *>(&levels[0]),size * sizeof(levels[0]));
        }

        readBinary(size, buf);
        if (size > 0)
        {
            intervals.reserve(size);
            for (size_t i = 0; i < size; i++)
            {
                Times times;
                readBinary(times, buf);
                intervals.emplace_back(times);
            }
        }

        readBinary(has_null, buf);
        readBinary(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            Numeric p;
            UInt32 ind;
            readBinary(p, buf);
            readBinary(ind, buf);
            groups[p] = ind;
        }
    }
};

template <typename ParamType>
struct AggregateFunctionFinderFunnelStringGroupData
{
    using Allocator = MixedArenaAllocator<4096>;
    EventLists<ParamType, Int32> event_lists;
    LEVELs levels;
    std::vector<Times> intervals;
    StringMapDict<UInt32> dict_index;
    bool has_null = false;
    bool sorted = true;

    UInt32 addProp(StringRef &p, Arena *arena)
    {
        UInt32 index;
        auto res = dict_index.get(p);
        if (res.first)
        {
            index = res.second;
        }
        else
        {
            index = dict_index.size();
            dict_index.add(p, arena);
        }

        return index;
    }

    template<bool need_order = false>
    void sort()
    {
        if (!sorted)
        {
            auto compare = [](const TimeEventWithProInd<ParamType, Int32> &left, const TimeEventWithProInd<ParamType, Int32> &right) {
                if constexpr (need_order)
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event) || (left.ctime == right.ctime && left.event == right.event && left.stime < right.stime);
                else
                    return (left.ctime < right.ctime) || (left.ctime == right.ctime && left.event < right.event);
            };

            std::sort(std::begin(event_lists), std::end(event_lists), compare);
            sorted = true;
        }
    }

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag, StringRef &p, bool is_null, ParamType &attr, Arena *arena)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        if (is_null)
        {
            event_lists.template emplace_back(ctime, flag, stime, -1, true, attr);
            has_null = true;
        }
        else
            event_lists.template emplace_back(ctime, flag, stime, addProp(p, arena), false, attr);
    }

    template<bool need_order = false>
    void add(UInt32 stime, UInt64 ctime, UInt64 flag, StringRef& p, bool is_null, Arena *arena)
    {
        if constexpr (need_order)
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag) ||
                 (event_lists.back().ctime == ctime && event_lists.back().event == flag && event_lists.back().stime > stime)))
            {
                sorted = false;
            }
        }
        else
        {
            if (sorted && event_lists.size() > 0 &&
                (event_lists.back().ctime > ctime ||
                 (event_lists.back().ctime == ctime && event_lists.back().event > flag)))
            {
                sorted = false;
            }
        }

        if (event_lists.size() == 0)
        {
            event_lists.reserve(INIT_VECTOR_SIZE);
        }

        if (is_null)
        {
            event_lists.template emplace_back(ctime, flag, stime, -1, true);
            has_null = true;
        }
        else
            event_lists.template emplace_back(ctime, flag, stime, addProp(p, arena), false);
    }

    void merge(const AggregateFunctionFinderFunnelStringGroupData<ParamType> &rhs, Arena *arena)
    {
        // The scenario is a bit complex here because there isn't global user
        // property dictionary, the merge process will looks like this:
        // 1. merge local dictionary
        // 2. update record using local dictionary by merged dictionary
        // 3. merge the data
        has_null |= rhs.has_null;
        auto & other_noconst = const_cast<AggregateFunctionFinderFunnelStringGroupData<ParamType> &>(rhs);
        UInt32 prev_user_pro_index = 0xFFFFFFFF;
        UInt32 prev_new_user_pro_index = 0xFFFFFFFF;

        for (auto& di : rhs.dict_index.getRawBuf())
            dict_index.add(di.first, arena);

        auto& old_dict_index = other_noconst.dict_index;
        for (auto& ev : other_noconst.event_lists)
        {
            if (!ev.is_null)
            {
                // record previous relocate info to avoid recomputing
                if (ev.pro_ind == int(prev_user_pro_index))
                {
                    ev.pro_ind = prev_new_user_pro_index;
                }
                else
                {
                    prev_user_pro_index = ev.pro_ind;
                    StringRef str = locateMapDictByIndex(old_dict_index, prev_user_pro_index);
                    ev.pro_ind = dict_index[str];
                    prev_new_user_pro_index = ev.pro_ind;
                }
            }
        }
        std::copy(std::move_iterator(rhs.event_lists.begin()), std::move_iterator(rhs.event_lists.end()), std::back_inserter(event_lists));
        sorted = false;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        size_t size = event_lists.size();
        writeBinary(size, buf);
        for (const auto& event : event_lists)
        {
            writeBinary(event.ctime, buf);
            writeBinary(event.event, buf);
            writeBinary(event.stime, buf);
            writeBinary(event.pro_ind, buf);
            writeBinary(event.param, buf);
            writeBinary(event.is_null, buf);
        }

        size = levels.size();
        writeBinary(size, buf);
        if (size > 0)
            buf.write(reinterpret_cast<const char *>(&levels[0]),size * sizeof(levels[0]));

        size = intervals.size();
        writeBinary(size, buf);
        if (size > 0)
        {
            for (const auto& times : intervals)
                writeBinary(times, buf);
        }

        writeBinary(has_null, buf);
        //also need serialize user property dictionary
        size = dict_index.size();
        writeBinary(size, buf);
        for (const auto& kv : dict_index.getRawBuf())
        {
            writeBinary(kv.first, buf); // serialize StringRef
            writeBinary(kv.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena* arena)
    {
        size_t size;
        readBinary(sorted, buf);
        readBinary(size, buf);
        event_lists.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            TimeEventWithProInd<ParamType, Int32> event;
            readBinary(event.ctime, buf);
            readBinary(event.event, buf);
            readBinary(event.stime, buf);
            readBinary(event.pro_ind, buf);
            readBinary(event.param, buf);
            readBinary(event.is_null, buf);

            event_lists.template emplace_back(event);
        }

        readBinary(size, buf);
        if (size > 0)
        {
            levels.resize(size, arena);
            buf.read(reinterpret_cast<char *>(&levels[0]),size * sizeof(levels[0]));
        }

        readBinary(size, buf);
        if (size > 0)
        {
            intervals.reserve(size);
            for (size_t i = 0; i < size; i++)
            {
                Times times;
                readBinary(times, buf);
                intervals.emplace_back(times);
            }
        }

        readBinary(has_null, buf);
        readBinary(size, buf);
        auto& dict_raw_buf = dict_index.getRawBuf();
        dict_raw_buf.resize(size, arena);
        // Data Type should match those used in write logic
        UInt32 index;
        for (UInt32 i = 0; i < size; i++)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            readBinary(index, buf);
            dict_raw_buf[i] = {ref, index};
        }
    }
};

inline static UInt64 setValidWindow(UInt64 first_time, const DateLUTImpl & dateLut)
{
    UInt64 time_in_sec = first_time/1000;
    auto year = dateLut.toYear(time_in_sec);
    auto month = dateLut.toMonth(time_in_sec);
    auto day = dateLut.toDayOfMonth(time_in_sec);
    auto timestamp_end_the_day = dateLut.makeDateTime(year, month, day, 23, 59, 59);
    return 1000ULL * (timestamp_end_the_day - time_in_sec);
}

template<typename AttrType>
inline AttrType getAttribution(const IColumn ** columns, size_t row_num, int i)
{
    if constexpr (std::is_same<AttrType, String>::value)
        return (dynamic_cast<const ColumnString *>(columns[i]))->getDataAt(row_num).toString();
    else
        return static_cast<const ColumnVector<AttrType> *>(columns[i])->getData()[row_num];
}

inline bool isNextLevel(UInt32 ev, size_t level)
{
    return ev & EventMask[level];
}

template<typename Num>
void insertNestedVectorNumberIntoColumn(ColumnArray& vec_to, const std::vector<std::vector<Num>>& vec)
{
    Array array;
    for (const auto& item : vec)
    {
        Array arr;
        for (const auto& i : item)
            arr.push_back(i);
        array.push_back(arr);
    }
    vec_to.insert(array);
}

using REPType = UInt64;
class AggregateFunctionFunnelRepData
{
public:
    // using Array = PODArray<REPType, 32>;
    // Array value;
    REPType value[1];
};

class AggregateFunctionFunnelRep2Data
{
public:
    std::vector<REPType> value;
    std::vector<Arithmetics> ariths;
};

// ========================= For funnelPathSplit: ============================

template <typename T>
using Vector = std::vector<T, TrackAllocator<T>>;


struct ExtraProp
{
    bool is_null;
    StringRef prop;
};
using ExtraPropVec = Vector<ExtraProp>;

using EventIndex = UInt16;
using EventId = Int32;
using Time = UInt64;

struct NodeInfo
{
    EventId id{};
    StringRef prop{};
};

struct PathInfo
{
    Vector<NodeInfo> path;
    size_t level{};

    void clear()
    {
        Vector<NodeInfo>{}.swap(path);
        level = 0;
    }
};

struct PathInfoPro
{
    Vector<NodeInfo> path;
    size_t level{};
    Time begin_time{};
};

using PathBuckets = Vector<PathInfoPro>;
// FunnelLevelMap: unordered_map(level, set(bucket_index))
using FunnelLevelMap = std::unordered_map<size_t, std::set<size_t>>;

struct Event
{
    EventIndex index;
    Time time;
    StringRef param;
    ExtraPropVec extra_prop;
    Event(EventIndex index_, Time time_, StringRef param_, ExtraPropVec & extra_prop_) : index(index_), time(time_), param(param_), extra_prop(std::move(extra_prop_)) { }
};

struct AggregateFunctionFunnelPathSplitData
{
    using Allocator = MixedArenaAllocator<8192>;
    using Events = PODArray<Event, 32 * sizeof(Event), Allocator>;
    Events events;

    bool sorted = false;
    void sort(bool reverse = false)
    {
        if (sorted)
            return;
        if (reverse)
        {
            pdqsort(events.begin(), events.end(), [](Event & lhs, Event & rhs) {
                return lhs.time > rhs.time
                    || (lhs.time == rhs.time && (lhs.index > rhs.index || (lhs.index == rhs.index && lhs.param > rhs.param)));
            });
        }
        else
        {
            pdqsort(events.begin(), events.end(), [](Event & lhs, Event & rhs) {
                return lhs.time < rhs.time
                    || (lhs.time == rhs.time && (lhs.index < rhs.index || (lhs.index == rhs.index && lhs.param < rhs.param)));
            });
        }
        sorted = true;
    }

    void add(EventIndex index, Time time, StringRef param, ExtraPropVec & extra_prop, Arena * arena) { events.push_back(Event(index, time, param, extra_prop), arena); }
    
    void merge(const AggregateFunctionFunnelPathSplitData & other, Arena * arena)
    {
        sorted = false;
        size_t size = events.size();
        events.insert(std::begin(other.events), std::end(other.events), arena);
        // realloc from arena
        /// TODO: need it? 
        for (size_t i = size; i < events.size(); ++i)
        {
            auto t_param = events[i].param;
            events[i].param = StringRef(arena->insert(t_param.data, t_param.size), t_param.size);

            for (size_t j = 0; j < events[i].extra_prop.size(); ++j)
            {
                auto extra_prop = events[i].extra_prop[j];
                events[i].extra_prop[j].prop = StringRef(arena->insert(extra_prop.prop.data, extra_prop.prop.size), extra_prop.prop.size);
                events[i].extra_prop[j].is_null = extra_prop.is_null;
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        size_t size = events.size();
        writeBinary(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            writeBinary(events[i].index, buf);
            writeBinary(events[i].time, buf);
            writeBinary(events[i].param, buf);

            size_t extra_size = events[i].extra_prop.size();
            writeBinary(extra_size, buf);
            for (size_t j = 0; j < extra_size; ++j)
            {
                writeBinary(events[i].extra_prop[j].is_null, buf);
                writeBinary(events[i].extra_prop[j].prop, buf);
            }
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
            EventIndex index;
            Time time;
            readBinary(index, buf);
            readBinary(time, buf);
            StringRef param = readStringBinaryInto(*arena, buf);

            size_t extra_size;
            readBinary(extra_size, buf);
            ExtraPropVec extra_prop;
            for (size_t j = 0; j < extra_size; ++j)
            {
                bool is_null;
                readBinary(is_null, buf);
                StringRef prop = readStringBinaryInto(*arena, buf);
                extra_prop.push_back(ExtraProp{is_null, prop});
            }

            add(index, time, param, extra_prop, arena);
        }
    }
};

inline bool isCommonEvent(UInt64 event_num)
{
    return !event_num;
}

inline bool isFunnelEvent(size_t level_flag, UInt64 event_num)
{
    return (!isCommonEvent(event_num)) && event_num <= level_flag;
}

bool nextLevelNeedPropNode(const std::vector<UInt64> & prop_flags, size_t current_level);

size_t getExtraPropIndex(const std::vector<UInt64> & prop_flags, size_t current_level);



}

