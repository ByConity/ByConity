#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class SingleDistinctAggregationToGroupBy : public Rule
{
public:
    RuleType getType() const override { return RuleType::SINGLE_DISTINCT_AGG_TO_GROUPBY; }
    String getName() const override { return "SINGLE_DISTINCT_AGG_TO_GROUPBY"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_single_distinct_to_group_by; }
    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
