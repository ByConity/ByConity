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

#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/DistributedPipelineSettings.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <cstddef>

namespace DB
{

struct Settings;
class PlanSegment;
struct PlanSegmentExecutionInfo;
struct PlanSegmentMultiPartitionSource;

struct BuildQueryPipelineSettings
{
    ExpressionActionsSettings actions_settings;
    DistributedPipelineSettings distributed_settings;
    ContextPtr context;
    bool is_expand = false;
    std::unordered_map<UInt64, std::vector<PlanSegmentMultiPartitionSource>> sources;

    const ExpressionActionsSettings & getActionsSettings() const { return actions_settings; }

    static BuildQueryPipelineSettings fromSettings(const Settings & from);
    static BuildQueryPipelineSettings fromContext(ContextPtr from);
    static BuildQueryPipelineSettings
    fromPlanSegment(PlanSegment * plan_segment, const PlanSegmentExecutionInfo & info, ContextPtr context, bool is_explain = false);
};
}
