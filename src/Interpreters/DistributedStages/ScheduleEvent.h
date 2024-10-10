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
    TriggerDispatch = 3,
    WorkerRestarted = 4,
    SegmentInstanceFinished = 5,
    ResendResource = 6
};

struct ScheduleEvent
{
    ScheduleEvent() = default;
    ScheduleEvent(ScheduleEvent & event) = default;
    ScheduleEvent & operator=(const ScheduleEvent &) = default;
    virtual ~ScheduleEvent() = default;

    virtual ScheduleEventType getType() const
    {
        return ScheduleEventType::Unknown;
    }
};

struct AbortEvent : ScheduleEvent
{
    explicit AbortEvent(const String & error_msg_, int code_ = ErrorCodes::LOGICAL_ERROR) : error_msg(error_msg_), code(code_)
    {
    }
    ScheduleEventType getType() const override
    {
        return ScheduleEventType::Abort;
    }
    String error_msg;
    int code;
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

    ScheduleEventType getType() const override
    {
        return ScheduleEventType::ScheduleBatchTask;
    }
    BatchTaskPtr batch_task;
};

struct TriggerDispatchEvent : ScheduleEvent
{
    explicit TriggerDispatchEvent(std::vector<WorkerNode> available_workers) : workers(std::move(available_workers))
    {
    }
    TriggerDispatchEvent() : all_workers(true)
    {
    }

    ScheduleEventType getType() const override
    {
        return ScheduleEventType::TriggerDispatch;
    }

    std::vector<WorkerNode> workers;
    bool all_workers{false};
};

struct WorkerRestartedEvent : ScheduleEvent
{
    WorkerRestartedEvent(const WorkerId & worker_id_, UInt32 register_time_) : worker_id(worker_id_), register_time(register_time_)
    {
    }

    ScheduleEventType getType() const override
    {
        return ScheduleEventType::WorkerRestarted;
    }

    WorkerId worker_id;
    UInt32 register_time;
};

struct SegmentInstanceFinishedEvent : ScheduleEvent
{
    SegmentInstanceFinishedEvent(size_t segment_id_, UInt64 parallel_index_, const RuntimeSegmentStatus & status_)
        : segment_id(segment_id_), parallel_index(parallel_index_), status(status_)
    {
    }

    ScheduleEventType getType() const override
    {
        return ScheduleEventType::SegmentInstanceFinished;
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

    ScheduleEventType getType() const override
    {
        return ScheduleEventType::ResendResource;
    }

    const HostWithPorts host_ports;
};

} // namespace DB
