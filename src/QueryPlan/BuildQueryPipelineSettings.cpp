#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

namespace DB
{

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromSettings(const Settings & from)
{
    BuildQueryPipelineSettings settings;
    settings.actions_settings = ExpressionActionsSettings::fromSettings(from, CompileExpressions::yes);
    return settings;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromContext(ContextPtr from)
{
    auto settings = fromSettings(from->getSettingsRef());
    settings.context = from;
    return settings;
}

BuildQueryPipelineSettings BuildQueryPipelineSettings::fromPlanSegment(PlanSegment * plan_segment, ContextPtr context)
{
    auto settings = fromContext(context);
    settings.distributed_settings = DistributedPipelineSettings::fromPlanSegment(plan_segment);
    settings.context = context;
    return settings;
}

}
