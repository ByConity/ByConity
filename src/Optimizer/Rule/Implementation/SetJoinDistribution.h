#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class SetJoinDistribution : public Rule
{
public:
    RuleType getType() const override { return RuleType::SET_JOIN_DISTRIBUTION; }
    String getName() const override { return "SET_JOIN_DISTRIBUTION"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
