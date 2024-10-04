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

#include <Optimizer/Rule/Transformation/InlineCTE.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <Optimizer/Rewriter/ColumnPruning.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/UnifyJoinOutputs.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rules.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
ConstRefPatternPtr InlineCTE::getPattern() const
{
    static auto pattern = Patterns::cte().result();
    return pattern;
}

TransformResult InlineCTE::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * cte_step = dynamic_cast<const CTERefStep *>(node->getStep().get());
    if (cte_step->hasFilter())
        return {}; // InlineCTEWithFilter

    auto inlined_plan = cte_step->toInlinedPlanNode(context.cte_info, context.context);
    return InlineCTE::reoptimize(cte_step->getId(), inlined_plan, context.cte_info, context.context);
}

ConstRefPatternPtr InlineCTEWithFilter::getPattern() const
{
    static auto pattern = Patterns::filter().withSingle(Patterns::cte()).result();
    return pattern;
}

TransformResult InlineCTEWithFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto cte = node->getChildren()[0];
    const auto * cte_step = dynamic_cast<const CTERefStep *>(cte->getStep().get());
    if (!cte_step->hasFilter())
        return {}; // InlineCTE

    auto inlined_plan
        = PlanNodeBase::createPlanNode(node->getId(), node->getStep(), {cte_step->toInlinedPlanNode(context.cte_info, context.context)});
    return InlineCTE::reoptimize(cte_step->getId(), inlined_plan, context.cte_info, context.context);
}

PlanNodePtr InlineCTE::reoptimize(CTEId cte_id, const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context)
{
    if (context->getSettingsRef().print_graphviz)
        GraphvizPrinter::printLogicalPlan(
            *node, context, std::to_string(context->getRuleId()) + "_cte_" + std::to_string(cte_id) + "_inlined");

    static Rewriters rewriters
        = {std::make_shared<ColumnPruning>(),
           std::make_shared<PredicatePushdown>(false, true),
           std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
           std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression"),
           std::make_shared<IterativeRewriter>(Rules::swapPredicateRules(), "SwapPredicate"),
           std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
           std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
           std::make_shared<UnifyJoinOutputs>()};

    QueryPlan sub_plan{node, cte_info, context->getPlanNodeIdAllocator()};
    for (auto & rewriter : rewriters)
        rewriter->rewritePlan(sub_plan, context);
    return sub_plan.getPlanNode();
}
}
