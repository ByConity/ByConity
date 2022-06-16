#pragma once
#include <Optimizer/Rule/Rule.h>

#include <Optimizer/Rule/Transformation/MagicSetForAggregation.h>

namespace DB
{

class MagicSetPushThroughProject : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_PUSH_THROUGH_PROJECTION; }
    String getName() const override { return "MAGIC_SET_PUSH_THROUGH_PROJECTION"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/**
 * NOTE: MagicSetPushThroughJoin blocks JOIN_ENUM_ON_GRAPH
 */
class MagicSetPushThroughJoin : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_PUSH_THROUGH_JOIN; }
    String getName() const override { return "MAGIC_SET_PUSH_THROUGH_JOIN"; }

    PatternPtr getPattern() const override;

    const std::vector<RuleType> & blockRules() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    static std::optional<PlanNodePtr> trySwapJoin(const PlanNodePtr & node, RuleContext & context);
};

class MagicSetPushThroughFilter : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_PUSH_THROUGH_FILTER; }
    String getName() const override { return "MAGIC_SET_PUSH_THROUGH_FILTER"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MagicSetPushThroughAggregating : public MagicSetRule
{
public:
    RuleType getType() const override { return RuleType::MAGIC_SET_PUSH_THROUGH_AGGREGATING; }
    String getName() const override { return "MAGIC_SET_PUSH_THROUGH_AGGREGATING"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
