#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class PullLeftJoinThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_Left_JOIN_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_Left_JOIN_THROUGH_INNER_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


class PullLeftJoinProjectionThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_Left_JOIN_PROJECTION_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_Left_JOIN_PROJECTION_THROUGH_INNER_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PullLeftJoinFilterThroughInnerJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_Left_JOIN_FILTER_THROUGH_INNER_JOIN; }
    String getName() const override { return "PULL_Left_JOIN_FILTER_THROUGH_INNER_JOIN"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
