#pragma once

#include <vector>
#include <Interpreters/DistributedStages/RuntimeSegmentsStatus.h>
#include <Interpreters/NodeSelector.h>
#include <common/types.h>

namespace DB
{
/// Indicates a plan segment.
struct SegmentTask
{
    explicit SegmentTask(size_t segment_id_, bool has_table_scan_, bool has_table_scan_or_value_)
        : segment_id(segment_id_), is_leaf(has_table_scan_), has_table_scan_or_value(has_table_scan_or_value_)
    {
    }
    // plan segment id.
    size_t segment_id;
    // segment containing only source input
    bool is_leaf;
    // segment containing source input
    bool has_table_scan_or_value;
};

using SegmentTaskPtr = std::shared_ptr<SegmentTask>;

// Tasks would be scheduled in same round.
using BatchTask = std::vector<SegmentTask>;
using BatchTaskPtr = std::shared_ptr<BatchTask>;

using BatchTasks = std::vector<BatchTaskPtr>;
using BatchTasksPtr = std::shared_ptr<BatchTasks>;

enum class ScheduleEventType : uint8_t
{
    Unknown = 0,
    Abort = 1,
    ScheduleBatchTask = 2,
    DispatchTrigger = 3,
    WorkerRestart = 4,
    SegmentInstanceFinish = 5,
    ResendResource = 6
};

struct ScheduleEvent
{
    ScheduleEvent() = default;
    ScheduleEvent(ScheduleEvent & event) = default;
    ScheduleEvent & operator=(const ScheduleEvent &) = default;
    virtual ~ScheduleEvent() = default;

    virtual ScheduleEventType getType()
    {
        return ScheduleEventType::Unknown;
    }
};

struct AbortEvent : ScheduleEvent
{
    ScheduleEventType getType() override
    {
        return ScheduleEventType::Abort;
    }
    String error_msg;
};

struct ScheduleBatchTaskEvent : ScheduleEvent
{
    explicit ScheduleBatchTaskEvent(BatchTaskPtr batch_task_)
    {
        batch_task = batch_task_;
    }
    ScheduleBatchTaskEvent(ScheduleBatchTaskEvent & event) = default;
    ScheduleBatchTaskEvent & operator=(const ScheduleBatchTaskEvent &) = default;
    ~ScheduleBatchTaskEvent() override = default;

    ScheduleEventType getType() override
    {
        return ScheduleEventType::ScheduleBatchTask;
    }
    BatchTaskPtr batch_task;
};

struct DispatchTriggerEvent : ScheduleEvent
{
    explicit DispatchTriggerEvent(std::vector<WorkerNode> available_workers) : workers(std::move(available_workers))
    {
    }
    DispatchTriggerEvent() : all_workers(true)
    {
    }

    ScheduleEventType getType() override
    {
        return ScheduleEventType::DispatchTrigger;
    }

    std::vector<WorkerNode> workers;
    bool all_workers{false};
};

struct WorkerRestartEvent : ScheduleEvent
{
    WorkerRestartEvent(const WorkerId & worker_id_, UInt32 register_time_) : worker_id(worker_id_), register_time(register_time_)
    {
    }

    ScheduleEventType getType() override
    {
        return ScheduleEventType::WorkerRestart;
    }

    WorkerId worker_id;
    UInt32 register_time;
};

struct SegmentInstanceFinishEvent : ScheduleEvent
{
    SegmentInstanceFinishEvent(size_t segment_id_, UInt64 parallel_index_, const RuntimeSegmentStatus & status_)
        : segment_id(segment_id_), parallel_index(parallel_index_), status(status_)
    {
    }

    ScheduleEventType getType() override
    {
        return ScheduleEventType::SegmentInstanceFinish;
    }

    size_t segment_id;
    UInt64 parallel_index;
    RuntimeSegmentStatus status;
};

struct ResendResourceEvent : ScheduleEvent
{
    explicit ResendResourceEvent(const HostWithPorts & host_ports_) : host_ports(host_ports_)
    {
    }

    ScheduleEventType getType() override
    {
        return ScheduleEventType::ResendResource;
    }

    const HostWithPorts host_ports;
};

} // namespace DB
