#pragma once

#include <Optimizer/Rule/Rule.h>
#include <Optimizer/Rewriter/Rewriter.h>

namespace DB
{
class InlineCTE : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_CTE; }
    String getName() const override { return "INLINE_CTE"; }
    bool isEnabled(ContextPtr context) override { return context->getSettingsRef().cte_mode == CTEMode::AUTO; }

    PatternPtr getPattern() const override;
protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    static PlanNodePtr rewriteSubPlan(const PlanNodePtr & node, CTEInfo & cte_info, ContextMutablePtr & context);
};
}
