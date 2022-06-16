#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/DistributedPipelineSettings.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <cstddef>

namespace DB
{

struct Settings;
class PlanSegment;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;
    DistributedPipelineSettings distributed_settings;
    ContextPtr context;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }

    static BuildQueryPipelineSettings fromSettings(const Settings & from);
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
    static BuildQueryPipelineSettings fromPlanSegment(PlanSegment * plan_segment, ContextPtr context);
};

}
