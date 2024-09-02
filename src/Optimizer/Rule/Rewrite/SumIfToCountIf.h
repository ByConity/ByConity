#pragma once
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/ExpressionInterpreter.h>

namespace DB
{

class SumIfToCountIf : public Rule
{
public:
    RuleType getType() const override { return RuleType::SUM_IF_TO_COUNT_IF; }
    String getName() const override { return "SumIfToCountIf"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_sum_if_to_count_if; }
    ConstRefPatternPtr getPattern() const override;
    virtual bool excludeIfTransformSuccess() const override { return true; }
    virtual bool excludeIfTransformFailure() const override { return true; }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
