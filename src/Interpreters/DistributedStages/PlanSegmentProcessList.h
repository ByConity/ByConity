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


#include <Core/Types.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <parallel_hashmap/phmap.h>
#include <Poco/Logger.h>
#include <common/types.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>

namespace DB
{
class ProcessListEntry;
class QueryStatus;

/// Group contains running queries created from one distributed query.
class PlanSegmentGroup
{
public:
    using Element = std::shared_ptr<ProcessListEntry>;
    using Container = std::map<UInt32, Element>;

    PlanSegmentGroup(String coordinator_address_, Decimal64 initial_query_start_time_ms_, bool use_query_memory_tracker_, size_t queue_bytes_)
        : coordinator_address(std::move(coordinator_address_))
        , initial_query_start_time_ms(initial_query_start_time_ms_)
        , use_query_memory_tracker(use_query_memory_tracker_)
    {
        if (queue_bytes_ != 0)
            memory_controller = std::make_shared<MemoryController>(queue_bytes_);
    }

    bool empty()
    {
        std::unique_lock lock(mutex);
        return segment_queries.empty();
    }

    bool emplace(UInt32 segment_id, Element && element)
    {
        std::unique_lock lock(mutex);
        const auto ret = segment_queries.insert_or_assign(segment_id, std::move(element));
        return ret.second;
    }

    void erase(UInt32 segment_id)
    {
        std::unique_lock lock(mutex);
        segment_queries.erase(segment_id);
    }

    bool contains(UInt32 segment_id)
    {
        std::unique_lock lock(mutex);
        return segment_queries.find(segment_id) != segment_queries.end();
    }

    bool tryCancel(bool internal);

    bool tryCancel(UInt32 segment_id, bool internal);

    mutable bthread::Mutex mutex;
    String coordinator_address;
    Decimal64 initial_query_start_time_ms{0};
    Container segment_queries;
    // not for planSegment_0, because query_context of planSegment_0 create too early
    bool use_query_memory_tracker{true};
    MemoryTracker memory_tracker{VariableContext::Process};
    // for all planSegment
    std::shared_ptr<MemoryController> memory_controller = nullptr;
};

class PlanSegmentProcessList;
class PlanSegmentProcessListEntry
{
private:
    using Element = std::shared_ptr<QueryStatus>;
    PlanSegmentProcessList & parent;
    Element status;
    String initial_query_id;
    UInt32 segment_id;
    AddressInfo coordinator_address;
    std::shared_ptr<MemoryController> memory_controller;

public:
    PlanSegmentProcessListEntry(PlanSegmentProcessList & parent_, Element status_, String initial_query_id_, UInt32 segment_id_);
    ~PlanSegmentProcessListEntry();
    Element operator->() { return status; }
    const Element operator->() const { return status; }
    QueryStatus & get() { return *status; }
    const QueryStatus & get() const { return *status; }
    void setCoordinatorAddress(const AddressInfo & coordinator_address_) {coordinator_address = coordinator_address_;}
    AddressInfo getCoordinatorAddress() const {return coordinator_address;}
    void setMemoryController(std::shared_ptr<MemoryController> & memory_controller_) {memory_controller = memory_controller_;}
    std::shared_ptr<MemoryController> getMemoryController() const {return memory_controller;}
    UInt32 getSegmentId() const
    {
        return segment_id;
    }
};

/// List of currently executing query created from plan segment.
class PlanSegmentProcessList
{
public:
    /// distributed query_id -> GroupIdToElement(s). There can be multiple queries with the same query_id as long as all queries except one are cancelled.
    using EntryPtr = std::shared_ptr<PlanSegmentProcessListEntry>;

    using Element = std::shared_ptr<PlanSegmentGroup>;

    friend class PlanSegmentProcessListEntry;

    Element insertGroup(const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force = false);

    EntryPtr insertProcessList(const Element segment_group, const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force = false);

    CancellationCode tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address = "");

    bool remove(const String & initial_query_id, UInt32 segment_id, bool before_execute = false);

private:
    bool tryEraseGroup();
    bool shouldEraseGroup();
    mutable bthread::Mutex group_to_erase_mutex;
    std::set<String> group_to_erase;
    size_t group_to_erase_threshold{64};

    using Container = phmap::parallel_flat_hash_map<std::string, Element,
            phmap::priv::hash_default_hash<std::string>,
            phmap::priv::hash_default_eq<std::string>,
            std::allocator<std::pair<std::string, Element>>,
            4, bthread::Mutex>;
    Container initail_query_to_groups;
    mutable bthread::Mutex mutex;
    mutable bthread::ConditionVariable remove_group;
    Poco::Logger * logger = &Poco::Logger::get("PlanSegmentProcessList");
};

}
