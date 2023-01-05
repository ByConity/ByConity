#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class PushPartialAggThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_AGG_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_AGG_THROUGH_EXCHANGE"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialSortingThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_SORTING_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_SORTING_THROUGH_EXCHANGE"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushPartialLimitThroughExchange : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PARTIAL_LIMIT_THROUGH_EXCHANGE; }
    String getName() const override { return "PUSH_PARTIAL_LIMIT_THROUGH_EXCHANGE"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
