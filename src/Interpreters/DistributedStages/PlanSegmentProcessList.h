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
    using Container = std::unordered_map<size_t, Element>;

    PlanSegmentGroup(
        const String & initial_query_id_,
        const String & coordinator_address_,
        Decimal64 initial_query_start_time_ms_,
        bool use_query_memory_tracker_,
        size_t queue_bytes_,
        const String & parent_initial_query_id_,
        bool is_internal_query_)
        : initial_query_id(std::move(initial_query_id_))
        , coordinator_address(std::move(coordinator_address_))
        , initial_query_start_time_ms(initial_query_start_time_ms_)
        , use_query_memory_tracker(use_query_memory_tracker_)
        , parent_initial_query_id(parent_initial_query_id_)
        , is_internal_query(is_internal_query_)
    {
        if (queue_bytes_ != 0)
            memory_controller = std::make_shared<MemoryController>(queue_bytes_);
    }

    ~PlanSegmentGroup()
    {
        if (use_query_memory_tracker)
            memory_tracker.reset(); // prevent log twice when destruct

        if (memory_controller)
        {
            memory_controller->logPeakMemoryUsage();
            memory_controller.reset();
        }
    }

    bool empty()
    {
        std::unique_lock lock(mutex);
        return segment_queries.empty();
    }

    bool emplace_null(std::vector<size_t> segment_ids)
    {
        std::unique_lock lock(mutex);

        for (const auto segment_id : segment_ids)
        {
            const auto ret = segment_queries.try_emplace(segment_id, nullptr);
            if (!ret.second)
                return false;
        }
        return true;
    }

    bool modify(size_t segment_id, Element && element)
    {
        std::unique_lock lock(mutex);
        auto iter = segment_queries.find(segment_id);
        if (iter == segment_queries.end())
            return false;
        iter->second = std::move(element);
        return true;
    }

    void erase(UInt32 segment_id)
    {
        std::unique_lock lock(mutex);
        segment_queries.erase(segment_id);
    }

    bool tryCancel(bool internal);

    void addChildQuery(const String & child_initial_query_id);
    std::set<String> getChildrenQuery();

    mutable bthread::Mutex mutex;
    String initial_query_id;
    String coordinator_address;
    Decimal64 initial_query_start_time_ms{0};
    std::atomic_bool is_cancelling{false};
    Container segment_queries;
    // not for planSegment_0, because query_context of planSegment_0 create too early
    bool use_query_memory_tracker{true};
    MemoryTracker memory_tracker{VariableContext::Process};
    // for all planSegment
    std::shared_ptr<MemoryController> memory_controller = nullptr;
    // support subquery, for children to add itself into parent
    String parent_initial_query_id;
    std::set<String> children_initial_query_id;
    bool is_internal_query = false;
};

using PlanSegmentGroupPtr = std::shared_ptr<PlanSegmentGroup>;
class PlanSegmentProcessList;
class PlanSegmentProcessListEntry
{
private:
    PlanSegmentProcessList & parent;
    PlanSegmentGroupPtr segment_group;
    std::optional<CurrentThread::QueryScope> query_scope;
    std::weak_ptr<QueryStatus> status;
    String initial_query_id;
    UInt32 segment_id;
    AddressInfo coordinator_address;
    std::shared_ptr<MemoryController> memory_controller;

public:
    PlanSegmentProcessListEntry(
        PlanSegmentProcessList & parent_, PlanSegmentGroupPtr segment_group_, String initial_query_id_, size_t segment_id_);
    ~PlanSegmentProcessListEntry();

    void setQueryStatus(std::shared_ptr<QueryStatus> status_) { status = std::move(status_); }
    std::shared_ptr<QueryStatus> getQueryStatus() const { return status.lock(); }

    void setMemoryController(std::shared_ptr<MemoryController> & memory_controller_) {memory_controller = memory_controller_;}
    std::shared_ptr<MemoryController> getMemoryController() const {return memory_controller;}

    PlanSegmentGroupPtr getPlanSegmentGroup() const {return segment_group;}
    size_t getSegmentId() const {return segment_id;}

    void prepareQueryScope(ContextMutablePtr query_context);
};

/// List of currently executing query created from plan segment.
class PlanSegmentProcessList
{
public:
    /// distributed query_id -> GroupIdToElement(s). There can be multiple queries with the same query_id as long as all queries except one are cancelled.
    using EntryPtr = std::shared_ptr<PlanSegmentProcessListEntry>;

    friend class PlanSegmentProcessListEntry;

    EntryPtr insertGroup(ContextMutablePtr query_context, size_t segment_id, bool force = false);

    std::vector<EntryPtr> insertGroup(ContextMutablePtr query_context, std::vector<size_t> & segment_ids, bool force = false);

    void insertProcessList(EntryPtr plan_segment_process_entry, size_t segment_id, ContextMutablePtr query_context, bool force = false);

    CancellationCode tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address = "");

    bool remove(std::string initial_query_id, size_t segment_id);

    size_t size() const { return initail_query_to_groups.size(); }

private:
    PlanSegmentGroupPtr getGroup(const String & initial_query_id) const;

    bool tryCascadeCancel(PlanSegmentGroupPtr segment_group, bool internal);

    using Container = phmap::parallel_flat_hash_map<
        std::string,
        PlanSegmentGroupPtr,
        phmap::priv::hash_default_hash<std::string>,
        phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<std::string, PlanSegmentGroupPtr>>,
        4,
        bthread::Mutex>;
    Container initail_query_to_groups;
    mutable bthread::Mutex mutex;
    mutable bthread::ConditionVariable remove_group;
    LoggerPtr logger = getLogger("PlanSegmentProcessList");
};

}
