#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/Context.h>

namespace DB
{

BlockIO executePlanSegment(const PlanSegmentPtr & plan_segment, ContextMutablePtr)
{
    LOG_DEBUG(&Poco::Logger::get("executePlanSegment"), "execute PlanSegment:\n" + plan_segment->toString());
    BlockIO res;
    return res;
}


}
