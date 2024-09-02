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

#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>

namespace DB
{
class InlineCTE : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_CTE; }
    String getName() const override { return "INLINE_CTE"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().cte_mode == CTEMode::AUTO; }
    ConstRefPatternPtr getPattern() const override;

    /**
     * In order for cascades to calculate the right cost, some ruls need be applied for inlined plan, 
     * like PredicatePushDown, SimplifyExpression, RemoveRedundant and so on.
     */
    static PlanNodePtr reoptimize(CTEId cte_id, const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context);

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class InlineCTEWithFilter : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_CTE_WITH_FILTER; }
    String getName() const override { return "InlineCTEWithFilter"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().cte_mode == CTEMode::AUTO; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}
