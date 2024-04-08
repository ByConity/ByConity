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
    using Container = std::map<PlanSegmentInstanceId, Element>;

    PlanSegmentGroup(String coordinator_address_, Decimal64 initial_query_start_time_ms_)
        : coordinator_address(std::move(coordinator_address_)), initial_query_start_time_ms(initial_query_start_time_ms_) {}

    bool empty()
    {
        std::unique_lock lock(mutex);
        return segment_queries.empty();
    }

    bool emplace(PlanSegmentInstanceId instance_id, Element && element)
    {
        std::unique_lock lock(mutex);
        const auto ret = segment_queries.insert_or_assign(instance_id, std::move(element));
        return ret.second;
    }

    void erase(PlanSegmentInstanceId instance_id)
    {
        std::unique_lock lock(mutex);
        segment_queries.erase(instance_id);
    }

    bool contains(PlanSegmentInstanceId instance_id)
    {
        std::unique_lock lock(mutex);
        return segment_queries.find(instance_id) != segment_queries.end();
    }

    bool tryCancel();

    bool tryCancel(PlanSegmentInstanceId instance_id);

    mutable bthread::Mutex mutex;
    String coordinator_address;
    Decimal64 initial_query_start_time_ms{0};
    Container segment_queries;
};

class PlanSegmentProcessList;
class PlanSegmentProcessListEntry
{
private:
    using Element = std::shared_ptr<QueryStatus>;
    PlanSegmentProcessList & parent;
    Element status;
    String initial_query_id;
    PlanSegmentInstanceId instance_id;
    AddressInfo coordinator_address;

public:
    PlanSegmentProcessListEntry(
        PlanSegmentProcessList & parent_, Element status_, String initial_query_id_, PlanSegmentInstanceId instance_id_);
    ~PlanSegmentProcessListEntry();
    Element operator->() { return status; }
    const Element operator->() const { return status; }
    QueryStatus & get() { return *status; }
    const QueryStatus & get() const { return *status; }
    void setCoordinatorAddress(const AddressInfo & coordinator_address_) {coordinator_address = coordinator_address_;}
    AddressInfo getCoordinatorAddress() const {return coordinator_address;}
    size_t getSegmentId() const
    {
        return instance_id.segment_id;
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

    EntryPtr insert(const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force = false);

    CancellationCode tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address = "");

    bool remove(std::string initial_query_id, PlanSegmentInstanceId instance_id);

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
