#pragma once


#include <Core/Types.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <bthread/mtx_cv_base.h>
#include <Poco/Logger.h>
#include <common/types.h>

#include <memory>
#include <unordered_map>
#include <utility>

namespace DB
{
class ProcessListEntry;
class QueryStatus;

/// Group contains running queries created from one distributed query.
struct PlanSegmentGroup
{
    String coordinator_address;
    Decimal64 initial_query_start_time_ms;
    using SegmentElement = std::pair<std::shared_ptr<ProcessListEntry>, QueryStatus *>;
    using SegmentIdToElement = std::unordered_map<size_t, SegmentElement>;
    SegmentIdToElement segment_queries;
};

class PlanSegmentProcessList;
class PlanSegmentProcessListEntry
{
private:
    PlanSegmentProcessList & parent;
    QueryStatus * status;
    size_t segment_id;

public:
    explicit PlanSegmentProcessListEntry(PlanSegmentProcessList & parent_, QueryStatus * status_, size_t segment_id_);
    ~PlanSegmentProcessListEntry();
    QueryStatus * operator->() { return status; }
    const QueryStatus * operator->() const { return status; }
    QueryStatus & get() { return *status; }
    const QueryStatus & get() const { return *status; }
};

/// List of currently executing query created from plan segment.
class PlanSegmentProcessList
{
public:
    /// distributed query_id -> GroupIdToElement(s). There can be multiple queries with the same query_id as long as all queries except one are cancelled.
    using InitialQueryToSegmentGroup = std::unordered_map<String, PlanSegmentGroup>;

    using EntryPtr = std::unique_ptr<PlanSegmentProcessListEntry>;

    friend class PlanSegmentProcessListEntry;

    EntryPtr insert(const PlanSegment & plan_segment, ContextMutablePtr query_context, bool force = false);

    CancellationCode tryCancelPlanSegmentGroup(const String & initial_query_id, String coordinator_address = "");

private:
    std::unordered_map<String, PlanSegmentGroup> initail_query_to_groups;
    mutable bthread::Mutex mutex;
    mutable bthread::ConditionVariable remove_group;
    Poco::Logger * logger = &Poco::Logger::get("PlanSegmentProcessList");
};


}
