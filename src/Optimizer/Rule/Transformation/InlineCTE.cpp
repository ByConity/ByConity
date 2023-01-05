#include <Optimizer/Rule/Transformation/InlineCTE.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rules.h>
#include <QueryPlan/CTERefStep.h>

namespace DB
{
PatternPtr InlineCTE::getPattern() const
{
    return Patterns::cte();
}

TransformResult InlineCTE::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * with_step = dynamic_cast<const CTERefStep *>(node->getStep().get());
    auto inlined_plan = with_step->toInlinedPlanNode(context.cte_info, context.context, true);
    return {rewriteSubPlan(inlined_plan, context.cte_info, context.context)};
}

PlanNodePtr InlineCTE::rewriteSubPlan(const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context)
{
    static Rewriters rewriters
        = {std::make_shared<PredicatePushdown>(),
           std::make_shared<IterativeRewriter>(Rules::simplifyExpressionRules(), "SimplifyExpression"),
           std::make_shared<IterativeRewriter>(Rules::removeRedundantRules(), "RemoveRedundant"),
           std::make_shared<IterativeRewriter>(Rules::inlineProjectionRules(), "InlineProjection"),
           std::make_shared<IterativeRewriter>(Rules::normalizeExpressionRules(), "NormalizeExpression")};

    QueryPlan sub_plan{node, cte_info, context->getPlanNodeIdAllocator()};
    for (auto & rewriter : rewriters)
        rewriter->rewrite(sub_plan, context);
    return sub_plan.getPlanNode();
}
}

