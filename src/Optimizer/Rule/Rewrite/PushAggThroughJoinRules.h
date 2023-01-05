#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class PushAggThroughOuterJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_AGG_THROUGH_OUTER_JOIN; }
    String getName() const override { return "PUSH_AGG_THROUGH_OUTER_JOIN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

struct MappedAggregationInfo
{
    PlanNodePtr aggregation_node;
    std::unordered_map<String, String> symbolMapping;
};

}
