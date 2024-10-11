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
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

PlanSegmentProcessList::EntryPtr PlanSegmentProcessList::insertGroup(ContextMutablePtr query_context, size_t segment_id, bool force)
{
    std::vector<size_t> segment_ids{segment_id};
    auto entries = insertGroup(query_context, segment_ids, force);
    return entries[0];
}

std::vector<PlanSegmentProcessList::EntryPtr>
PlanSegmentProcessList::insertGroup(ContextMutablePtr query_context, std::vector<size_t> & segment_ids, bool force)
{
    auto & settings = query_context->getSettingsRef();
    const auto & client_info = query_context->getClientInfo();
    const String & initial_query_id = client_info.initial_query_id;
    const String & coordinator_address = extractExchangeHostPort(query_context->getCoordinatorAddress());
    const String & parent_initial_query_id = client_info.parent_initial_query_id;
    bool is_internal_query = query_context->isInternalQuery();
    bool need_wait_cancel = false;

    auto initial_query_start_time_ms = query_context->getClientInfo().initial_query_start_time_microseconds;
    {
        auto segment_group = getGroup(initial_query_id);
        if (segment_group
            && (segment_group->coordinator_address != coordinator_address
                || segment_group->initial_query_start_time_ms != initial_query_start_time_ms))
        {
            if (!force && (!settings.replace_running_query || segment_group->initial_query_start_time_ms > initial_query_start_time_ms))
                throw Exception(
                    "Distributed query with id = " + initial_query_id + " is already running.",
                    ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

            LOG_WARNING(
                logger,
                "Distributed query with id = {} will be replaced by other coordinator: {}",
                initial_query_id,
                query_context->getCoordinatorAddress().toString());

            need_wait_cancel = tryCascadeCancel(segment_group, false);
        }
    }

    if (!is_internal_query && !query_context->getProcessListEntry().lock())
        query_context->getProcessList().checkRunningQuery(query_context, false, force);

    if (need_wait_cancel)
    {
        std::unique_lock lock(mutex);
        auto replace_running_query_max_wait_ms = settings.replace_running_query_max_wait_ms.totalMilliseconds();
        if (!replace_running_query_max_wait_ms
            || !remove_group.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms), [&] {
                    bool inited = false;
                    bool found = initail_query_to_groups.if_contains(initial_query_id, [&](auto & it) {
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

    PlanSegmentGroupPtr segment_group;
    auto exists = [&](Container::value_type & v) {
        if (v.second->coordinator_address == coordinator_address && v.second->initial_query_start_time_ms == initial_query_start_time_ms)
        {
            bool emplace = v.second->emplace_null(segment_ids);
            if (emplace)
                segment_group = v.second;
        }
    };
    auto emplace = [&](const Container::constructor & ctor) {
        bool use_query_memory_tracker
            = settings.exchange_use_query_memory_tracker && !settings.bsp_mode && (segment_ids.size() != 1 || segment_ids[0] != 0);
        size_t queue_bytes = settings.exchange_queue_bytes;
        segment_group = std::make_shared<PlanSegmentGroup>(
            initial_query_id,
            coordinator_address,
            initial_query_start_time_ms,
            use_query_memory_tracker,
            queue_bytes,
            parent_initial_query_id,
            is_internal_query);
        segment_group->emplace_null(segment_ids);
        ctor(initial_query_id, segment_group);
    };

    bool create = initail_query_to_groups.lazy_emplace_l(initial_query_id, exists, emplace);
    if (!segment_group)
        throw Exception(
            "Distributed query with id = " + initial_query_id + " is already running and can't be stopped",
            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

    if (create && !parent_initial_query_id.empty())
    {
        auto parent_segment_group = getGroup(parent_initial_query_id);
        // It's not important to find a real parent.
        if (parent_segment_group)
            parent_segment_group->addChildQuery(initial_query_id);
    }

    std::vector<EntryPtr> entries;
    for (size_t segment_id : segment_ids)
    {
        auto entry = std::make_shared<PlanSegmentProcessListEntry>(*this, segment_group, initial_query_id, segment_id);
        entry->setMemoryController(segment_group->memory_controller);
        entries.emplace_back(std::move(entry));
    }
    return entries;
}

void PlanSegmentProcessList::insertProcessList(
    EntryPtr plan_segment_process_entry, size_t segment_id, ContextMutablePtr query_context, bool force)
{
    ProcessList::EntryPtr entry;
    auto context_process_list_entry = query_context->getProcessListEntry().lock();
    if (context_process_list_entry)
        entry = std::move(context_process_list_entry);
    else
        entry = query_context->getProcessList().insert("", nullptr, query_context, force);

    plan_segment_process_entry->setQueryStatus(entry->getPtr());
    const auto segment_group = plan_segment_process_entry->getPlanSegmentGroup();
    bool exist = segment_group->modify(segment_id, std::move(entry));
    if (!exist)
        throw Exception(
            fmt::format(
                "Distributed query {}@{}@{} doesn't contain segment_id {}",
                query_context->getInitialQueryId(),
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms,
                segment_id),
            ErrorCodes::LOGICAL_ERROR);
}

bool PlanSegmentGroup::tryCancel(bool internal)
{
    std::vector<std::shared_ptr<ProcessListEntry>> to_cancel;
    std::unique_lock lock(mutex);
    for (auto & it : segment_queries)
    {
        if (it.second)
            to_cancel.push_back(it.second);
    }
    lock.unlock();

    if (!is_cancelling)
    {
        is_cancelling = true;

        for (auto & entry : to_cancel)
        {
            entry->get().cancelQuery(true, internal);
        }
    }

    return !to_cancel.empty();
}

void PlanSegmentGroup::addChildQuery(const String & child_initial_query_id)
{
    std::unique_lock lock(mutex);
    children_initial_query_id.emplace(child_initial_query_id);
}

std::set<String> PlanSegmentGroup::getChildrenQuery()
{
    std::unique_lock lock(mutex);
    return children_initial_query_id;
}

bool PlanSegmentProcessList::remove(std::string initial_query_id, size_t segment_id)
{
    auto segment_group = getGroup(initial_query_id);
    if (segment_group)
    {
        if (segment_group->use_query_memory_tracker)
            segment_group->memory_tracker.logPeakMemoryUsage();
        segment_group->erase(segment_id);

        LOG_TRACE(
            logger,
            "Remove segment {} for distributed query {}@{}@{} from PlanSegmentProcessList",
            segment_id,
            initial_query_id,
            segment_group->coordinator_address,
            segment_group->initial_query_start_time_ms);

        if (segment_group->empty())
        {
            size_t num_erased
                = initail_query_to_groups.erase_if(initial_query_id, [](const Container::value_type & v) { return v.second->empty(); });

            LOG_TRACE(
                logger,
                "Remove {} segment group for distributed query {}@{}@{}",
                num_erased,
                initial_query_id,
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms);

            if (num_erased)
                remove_group.notify_all();
        }
        return true;
    }

    LOG_ERROR(logger, "Logical error: Cannot found query: {} in PlanSegmentProcessList", initial_query_id);
    return false;
}

CancellationCode PlanSegmentProcessList::tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address)
{
    auto res = CancellationCode::CancelSent;
    bool found = false;

    auto segment_group = getGroup(initial_query_id);
    if (segment_group)
    {
        if (coordinator_address.empty() || segment_group->coordinator_address == coordinator_address)
        {
            found = tryCascadeCancel(segment_group, true);
            LOG_DEBUG(
                logger,
                "Try cancel for distributed query[{}@{}@{}] from PlanSegmentProcessList, result is {}",
                initial_query_id,
                coordinator_address,
                segment_group->initial_query_start_time_ms,
                found);
        }
        else
        {
            LOG_WARNING(
                logger,
                "Fail to cancel distributed query[{}@{}@{}], coordinator_address doesn't match, seg coordinator address is {}",
                initial_query_id,
                coordinator_address,
                segment_group->coordinator_address,
                segment_group->initial_query_start_time_ms);
            return CancellationCode::CancelCannotBeSent;
        }
    }

    if (!found)
    {
        res = CancellationCode::NotFound;
    }
    return res;
}

bool PlanSegmentProcessList::tryCascadeCancel(PlanSegmentGroupPtr segment_group, bool internal)
{
    bool found = segment_group->tryCancel(internal);
    auto ids = segment_group->getChildrenQuery();
    for (const auto & id : ids)
    {
        auto child_segment_group = getGroup(id);
        child_segment_group->tryCancel(internal);
    }
    return found;
}

PlanSegmentGroupPtr PlanSegmentProcessList::getGroup(const String & initial_query_id) const
{
    PlanSegmentGroupPtr segment_group;
    initail_query_to_groups.if_contains(initial_query_id, [&](auto & it) { segment_group = it.second; });
    return segment_group;
}

PlanSegmentProcessListEntry::PlanSegmentProcessListEntry(
    PlanSegmentProcessList & parent_, PlanSegmentGroupPtr segment_group_, String initial_query_id_, size_t segment_id_)
    : parent(parent_), segment_group(segment_group_), initial_query_id(std::move(initial_query_id_)), segment_id(segment_id_)
{
}

PlanSegmentProcessListEntry::~PlanSegmentProcessListEntry()
{
    parent.remove(initial_query_id, segment_id);
}

void PlanSegmentProcessListEntry::prepareQueryScope(ContextMutablePtr query_context)
{
    if (segment_group->use_query_memory_tracker)
        query_scope.emplace(query_context, &segment_group->memory_tracker);
    else
        query_scope.emplace(query_context);
}
}
