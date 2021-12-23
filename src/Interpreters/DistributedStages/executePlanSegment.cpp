#include <string>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/ProcessList.h>
#include <Common/ThreadPool.h>

namespace DB
{

BlockIO executePlanSegment(PlanSegmentPtr plan_segment, ContextMutablePtr context)
{
    if (!plan_segment)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);
    PlanSegmentExecutor executor(std::move(plan_segment), std::move(context));
    return executor.lazyExecute();
}

void executePlanSegment(PlanSegmentPtr plan_segment, ContextMutablePtr context, bool is_async)
{
    if (!plan_segment)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);

    auto execute_func = [&]() {
        PlanSegmentExecutor executor(std::move(plan_segment), std::move(context));
        executor.execute();
    };

    if (is_async)
    {
        ThreadFromGlobalPool async_thread(std::move(execute_func));
        async_thread.detach();
        return;
    }

    execute_func();
}

}
