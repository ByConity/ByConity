#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{

class ImplementExceptRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::IMPLEMENT_EXCEPT; }
    String getName() const override { return "IMPLEMENT_EXCEPT"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class ImplementIntersectRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::IMPLEMENT_INTERSECT; }
    String getName() const override { return "IMPLEMENT_INTERSECT"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
