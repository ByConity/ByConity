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

#include <IO/WriteBufferFromString.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>
#include <Common/time.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

std::shared_ptr<PlanSegmentGroup>
PlanSegmentProcessList::insertGroup(const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force)
{
    const String & initial_query_id = plan_segment.getQueryId();
    const String & coordinator_address = extractExchangeHostPort(plan_segment.getCoordinatorAddress());
    bool need_wait_cancel = false;

    if (shouldEraseGroup())
    {
        tryEraseGroup();
    }

    auto & settings = query_context->getSettingsRef();
    bool bsp_mode = settings.bsp_mode;
    auto instance_id = query_context->getPlanSegmentInstanceId();
    auto initial_query_start_time_ms = query_context->getClientInfo().initial_query_start_time_microseconds;
    {
        Element segment_group;
        bool found = initail_query_to_groups.if_contains(initial_query_id, [&](auto & it){
            segment_group = it.second;
        });
        if (found)
        {
            tryEraseGroup();

            if (segment_group->coordinator_address != coordinator_address)
            {
                if (initail_query_to_groups.if_contains(initial_query_id, [&](auto & it) { segment_group = it.second; }))
                {
                    if (!force
                        && (!settings.replace_running_query
                            || segment_group->initial_query_start_time_ms > initial_query_start_time_ms))
                        throw Exception(
                            "Distributed query with id = " + initial_query_id + " is already running.",
                            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

                    LOG_WARNING(
                        logger,
                        "Distributed query with id = {} will be replaced by other coordinator: {}",
                        initial_query_id,
                        plan_segment.getCoordinatorAddress().toString());

                    need_wait_cancel = segment_group->tryCancel();
                }
            }
        }

        if (found && bsp_mode && segment_group->coordinator_address == coordinator_address && segment_group->tryCancel(instance_id))
        {
            std::unique_lock lock(mutex);
            auto query_expiration_ts = query_context->getQueryExpirationTimeStamp();
            if (!remove_group.wait_until(lock, timespec_to_timepoint(query_expiration_ts), [&] {
                    tryEraseGroup();
                    bool deleted = true;
                    initail_query_to_groups.if_contains(initial_query_id, [&](auto & it) {
                        if (it.second->coordinator_address == coordinator_address && it.second->contains(instance_id))
                            deleted = false;
                    });
                    return deleted;
                }))
            {
                throw Exception(
                    fmt::format(
                        "Distributed query with id = {} is already running and can't be stopped, query_expiration_ts is {}",
                        initial_query_id,
                        query_expiration_ts.tv_sec * 1000 + query_expiration_ts.tv_nsec / 1000000),
                    ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
            }
        }
    }

    if (!bsp_mode && !query_context->getProcessListEntry().lock())
        query_context->getProcessList().checkRunningQuery(query_context, false, force);

    if (need_wait_cancel)
    {
        std::unique_lock lock(mutex);
        const auto replace_running_query_max_wait_ms
            = settings.replace_running_query_max_wait_ms.totalMilliseconds();
        if (!replace_running_query_max_wait_ms
            || !remove_group.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms), [&] {
                    tryEraseGroup();
                    bool inited = false;
                    bool found = initail_query_to_groups.if_contains(initial_query_id, [&](auto & it){
                        if (it.second->coordinator_address == coordinator_address)
                            inited = true;
                    });
                    return !found || inited;
               }))
        {
            throw Exception(
                "Distributed query with id = " + initial_query_id + " is already running and can't be stopped",
                ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
        }
    }

    {
        Element segment_group;
        auto exists = [&](Container::value_type & v) {
            if (v.second->coordinator_address == coordinator_address)
            {
                segment_group = v.second;
                segment_group->emplace(instance_id, nullptr);
            }
        };
        auto emplace = [&](const Container::constructor & ctor) {
            bool use_query_memory_tracker = settings.exchange_use_query_memory_tracker && !bsp_mode && instance_id.segment_id != 0;
            size_t queue_bytes = settings.exchange_queue_bytes;
            segment_group = std::make_shared<PlanSegmentGroup>(coordinator_address, initial_query_start_time_ms, use_query_memory_tracker, queue_bytes);
            segment_group->emplace(instance_id, nullptr);
            ctor(initial_query_id, segment_group);
        };

        initail_query_to_groups.lazy_emplace_l(initial_query_id, exists, emplace);
        if (!segment_group)
            throw Exception(
                "Distributed query with id = " + initial_query_id + " is already running and can't be stopped",
                ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

        return segment_group;
    }
}

PlanSegmentProcessList::EntryPtr PlanSegmentProcessList::insertProcessList(const Element segment_group, const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force)
{
    const String & initial_query_id = plan_segment.getQueryId();
    WriteBufferFromOwnString pipeline_buffer;
    QueryPlan::ExplainPlanOptions options;
    plan_segment.getQueryPlan().explainPlan(pipeline_buffer, options);
    String pipeline_string = pipeline_buffer.str();

    ProcessList::EntryPtr entry;
    auto context_process_list_entry = query_context->getProcessListEntry().lock();
    if (context_process_list_entry)
        entry = std::move(context_process_list_entry);
    else
        entry = query_context->getProcessList().insert("\n" + pipeline_string, nullptr, query_context, force);

    auto instance_id = query_context->getPlanSegmentInstanceId();
    auto res = std::make_shared<PlanSegmentProcessListEntry>(*this, entry->getPtr(), initial_query_id, instance_id);
    res->setCoordinatorAddress(plan_segment.getCoordinatorAddress());
    res->setMemoryController(segment_group->memory_controller);

    segment_group->emplace(instance_id, std::move(entry));
    return res;
}

bool PlanSegmentGroup::tryCancel()
{
    std::vector<std::shared_ptr<ProcessListEntry>> to_cancel;
    std::unique_lock lock(mutex);
    for (auto & it : segment_queries)
    {
        if (it.second)
            to_cancel.push_back(it.second);
    }
    lock.unlock();

    for (auto & entry : to_cancel)
    {
        entry->get().cancelQuery(true);
    }

    return !to_cancel.empty();
}

bool PlanSegmentGroup::tryCancel(PlanSegmentInstanceId instance_id)
{
    std::unique_lock lock(mutex);
    auto iter = segment_queries.find(instance_id);
    auto to_cancel = iter == segment_queries.end() ? nullptr : iter->second;
    lock.unlock();

    if (to_cancel)
        to_cancel->get().cancelQuery(true);

    return to_cancel != nullptr;
}

bool PlanSegmentProcessList::shouldEraseGroup()
{
    std::unique_lock lock(group_to_erase_mutex);
    return group_to_erase.size() > group_to_erase_threshold;
}

bool PlanSegmentProcessList::tryEraseGroup()
{
    std::unique_lock lock(group_to_erase_mutex);
    if (!group_to_erase.empty())
    {
        Stopwatch watch;
        auto group_to_erase_copy = group_to_erase;
        for (const auto & initial_query_id : group_to_erase_copy)
        {
            auto num_erased = initail_query_to_groups.erase_if(
                initial_query_id, [](const Container::value_type & v) { return v.second->segment_queries.empty(); });
            if (num_erased)
                group_to_erase.erase(initial_query_id);
        }
        LOG_DEBUG(
            logger,
            "Finish tryEraseGroup, old_size:{}, new_size:{}, elapsed_seconds:{}",
            group_to_erase_copy.size(),
            group_to_erase.size(),
            watch.elapsedSeconds());
    }
    return true;
}

bool PlanSegmentProcessList::remove(std::string initial_query_id, PlanSegmentInstanceId instance_id, bool before_execute)
{
    Element segment_group;
    initail_query_to_groups.if_contains(initial_query_id, [&](auto & it){
        segment_group = it.second;
    });
    if (segment_group.get())
    {
        if (!segment_group->empty())
        {
            segment_group->erase(instance_id);

            LOG_TRACE(
                logger,
                "Remove {} for distributed query {}@{} from PlanSegmentProcessList",
                instance_id.toString(),
                initial_query_id,
                segment_group->coordinator_address);
        }

        if (segment_group->empty())
        {
            LOG_TRACE(logger, "Remove segment group for distributed query {}@{}", initial_query_id, segment_group->coordinator_address);
            std::unique_lock lock(group_to_erase_mutex);
            if (!before_execute)
            {
                if (segment_group->use_query_memory_tracker)
                {
                    segment_group->memory_tracker.logPeakMemoryUsage();
                    segment_group->memory_tracker.reset(); // prevent log twice when destruct
                }
                if (segment_group->memory_controller)
                {
                    segment_group->memory_controller->logPeakMemoryUsage();
                    segment_group->memory_controller.reset();
                }
            }
            group_to_erase.insert(initial_query_id);
            lock.unlock();
            remove_group.notify_all();
        }
        return true;
    }

    // shouldn't reach here.
    if (!before_execute)
        LOG_ERROR(logger, "Logical error: Cannot found query: {} in PlanSegmentProcessList", initial_query_id);
    return false;
}

CancellationCode PlanSegmentProcessList::tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address)
{
    auto res = CancellationCode::CancelSent;
    bool found = false;
    Element segment_group;

    initail_query_to_groups.if_contains(initial_query_id, [&](auto & it){
        segment_group = it.second;
    });
    if (segment_group.get())
    {
        if (coordinator_address.empty() || segment_group->coordinator_address == coordinator_address)
            found = segment_group->tryCancel();
        else
        {
            LOG_WARNING(
                logger,
                "Fail to cancel distributed query[{}@{}], coordinator_address doesn't match, seg coordinator address is {}",
                initial_query_id,
                coordinator_address,
                segment_group->coordinator_address
            );
            return CancellationCode::CancelCannotBeSent;
        }
    }

    if (!found)
    {
        res = CancellationCode::NotFound;
    }
    return res;
}

PlanSegmentProcessListEntry::PlanSegmentProcessListEntry(
    PlanSegmentProcessList & parent_, Element status_, String initial_query_id_, PlanSegmentInstanceId instance_id_)
    : parent(parent_), status(status_), initial_query_id(std::move(initial_query_id_)), instance_id(instance_id_)
{
}

PlanSegmentProcessListEntry::~PlanSegmentProcessListEntry()
{
    parent.remove(initial_query_id, instance_id);
}
}
