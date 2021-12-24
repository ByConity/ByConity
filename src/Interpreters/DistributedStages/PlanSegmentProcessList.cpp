#include <Interpreters/CancellationCode.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>
#include "Interpreters/DistributedStages/AddressInfo.h"

#include <memory>
#include <mutex>
#include <string>
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

PlanSegmentProcessList::EntryPtr
PlanSegmentProcessList::insert(const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force)
{
    const String & initial_query_id = plan_segment.getQueryId();
    const String & segment_id_str = std::to_string(plan_segment.getPlanSegmentId());
    std::vector<QueryStatus *> need_cancalled_queries;
    auto initial_query_start_time_ms = query_context->getClientInfo().initial_query_start_time_microseconds;
    {
        std::lock_guard lock(mutex);
        const auto segment_group_it = initail_query_to_groups.find(initial_query_id);
        if (segment_group_it != initail_query_to_groups.end())
        {
            if (segment_group_it->second.coordinator_address != extractExchangeStatusHostPort(plan_segment.getCoordinatorAddress())
                && segment_group_it->second.initial_query_start_time_ms <= initial_query_start_time_ms)
            {
                if (!force && !query_context->getSettingsRef().replace_running_query)
                    throw Exception(
                        "Distributed query with id = " + initial_query_id + " is already running.",
                        ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);

                LOG_WARNING(
                    logger,
                    "Distributed query with id = {} will be replaced by other coodinator: {}",
                    initial_query_id,
                    plan_segment.getCoordinatorAddress().toString());
                for (auto & segment_query : segment_group_it->second.segment_queries)
                {
                    need_cancalled_queries.push_back(&segment_query.second->get());
                }
            }
        }
    }

    if (need_cancalled_queries.empty())
    {
        ProcessList::EntryPtr entry = query_context->getProcessList().insert("segment: " + segment_id_str, nullptr, query_context, force);
        auto res = std::make_unique<PlanSegmentProcessListEntry>(*this, &entry->get(), plan_segment.getPlanSegmentId());

        std::lock_guard lock(mutex);
        const auto segment_group_it = initail_query_to_groups.find(initial_query_id);

        if (segment_group_it == initail_query_to_groups.end())
        {
            PlanSegmentGroup segment_group{
                .coordinator_address = extractExchangeStatusHostPort(plan_segment.getCoordinatorAddress()),
                .initial_query_start_time_ms = initial_query_start_time_ms,
                .segment_queries = {{plan_segment.getPlanSegmentId(), std::move(entry)}}};
            initail_query_to_groups.emplace(initial_query_id, std::move(segment_group));
            return res;
        }
        const auto emplace_res = segment_group_it->second.segment_queries.emplace(plan_segment.getPlanSegmentId(), std::move(entry));
        if (!emplace_res.second)
        {
            throw Exception("Exsited segment_id: " + segment_id_str + " for query: " + initial_query_id, ErrorCodes::LOGICAL_ERROR);
        }
        return res;
    }

    for (auto & query : need_cancalled_queries)
        query->cancelQuery(true);

    ProcessList::EntryPtr entry = query_context->getProcessList().insert("segment: " + segment_id_str, nullptr, query_context, force);
    auto res = std::make_unique<PlanSegmentProcessListEntry>(*this, &entry->get(), plan_segment.getPlanSegmentId());
    {
        std::unique_lock lock(mutex);
        const auto replace_running_query_max_wait_ms
            = query_context->getSettingsRef().replace_running_query_max_wait_ms.totalMilliseconds();
        if (!replace_running_query_max_wait_ms
            || !remove_group.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms), [&] {
                   return initail_query_to_groups.find(initial_query_id) == initail_query_to_groups.end();
               }))
        {
            throw Exception(
                "Distributed query with id = " + initial_query_id + " is already running and can't be stopped",
                ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
        }

        PlanSegmentGroup segment_group{
            .coordinator_address = extractExchangeStatusHostPort(plan_segment.getCoordinatorAddress()),
            .initial_query_start_time_ms = initial_query_start_time_ms,
        };
        segment_group.segment_queries.emplace(plan_segment.getPlanSegmentId(), std::move(entry));
        initail_query_to_groups.emplace(initial_query_id, std::move(segment_group));
    }
    return res;
}


CancellationCode PlanSegmentProcessList::tryCancelPlanSegmentGroup(const String & initial_query_id, String coodinator_address)
{
    std::vector<ProcessList::EntryPtr> need_cancalled_queries;
    {
        std::lock_guard lock(mutex);
        auto segment_group_it = initail_query_to_groups.find(initial_query_id);
        if (segment_group_it != initail_query_to_groups.end())
        {
            if (!coodinator_address.empty() && segment_group_it->second.coordinator_address != coodinator_address)
            {
                return CancellationCode::CancelCannotBeSent;
            }

            for (auto & segment_query : segment_group_it->second.segment_queries)
            {
                need_cancalled_queries.push_back(segment_query.second);
            }
        }
    }

    if (need_cancalled_queries.empty())
    {
        return CancellationCode::NotFound;
    }
    for (auto & query : need_cancalled_queries)
        query->get().cancelQuery(true);
    return CancellationCode::CancelSent;
}

PlanSegmentProcessListEntry::PlanSegmentProcessListEntry(PlanSegmentProcessList & parent_, QueryStatus * status_, size_t segment_id_)
    : parent(parent_), status(status_), segment_id(segment_id_)
{
}

PlanSegmentProcessListEntry::~PlanSegmentProcessListEntry()
{
    const String & inital_query_id = status->getClientInfo().initial_query_id;
    ProcessList::EntryPtr found_entry;
    {
        std::unique_lock lock(parent.mutex);

        const auto segment_group_it = parent.initail_query_to_groups.find(inital_query_id);

        if (segment_group_it == parent.initail_query_to_groups.end())
        {
            LOG_ERROR(parent.logger, "Logical error: Cannot found query: {} in PlanSegmentProcessList", inital_query_id);
            std::terminate();
        }

        PlanSegmentGroup & segment_group = segment_group_it->second;

        if (auto running_query = segment_group.segment_queries.find(segment_id); running_query != segment_group.segment_queries.end())
        {
            if (&running_query->second->get() == status)
            {
                found_entry = std::move(running_query->second);

                segment_group.segment_queries.erase(segment_id);
                LOG_TRACE(
                    parent.logger,
                    "Remove segment {} for distributed query {} @ {} from PlanSegmentProcessList",
                    segment_id,
                    inital_query_id,
                    segment_group.coordinator_address);
            }
        }

        if (!found_entry)
        {
            LOG_ERROR(
                parent.logger,
                "Logical error: cannot find segment by query id: {}, segment_id: {} in PlanSegmentProcessList",
                inital_query_id,
                segment_id);
            std::terminate();
        }

        if (segment_group.segment_queries.empty())
        {
            LOG_TRACE(
                parent.logger, "Remove segment group for distributed query {} @ {}", inital_query_id, segment_group.coordinator_address);
            parent.initail_query_to_groups.erase(segment_group_it);
            parent.remove_group.notify_all();
        }
    }
    found_entry.reset();
}
}
