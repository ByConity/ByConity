#include <memory>
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

    //LOG_TRACE(&Poco::Logger::get("executePlanSegment"), "EXECUTE\n" + plan_segment->toString());

    if (context->getSettingsRef().debug_plan_generation)
        return;
    
    auto executor = std::make_shared<PlanSegmentExecutor>(std::move(plan_segment), std::move(context));

    if (is_async)
    {
        ThreadFromGlobalPool async_thread([executor = std::move(executor)]() { executor->execute(); });
        async_thread.detach();
        return;
    }

    executor->execute();
}

}
