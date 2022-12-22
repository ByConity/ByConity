#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class MergeUnionRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_UNION; }
    String getName() const override { return "MERGE_UNION"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergeExceptRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_EXCEPT; }
    String getName() const override { return "MERGE_EXCEPT"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class MergeIntersectRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_INTERSECT; }
    String getName() const override { return "MERGE_INTERSECT"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
