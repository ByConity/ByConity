#pragma once
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{

class SwapAdjacentWindows : public Rule
{
public:
    RuleType getType() const override { return RuleType::SWAP_WINDOWS; }
    String getName() const override { return "SWAP_ADJACENT_WINDOWS"; }

    PatternPtr getPattern() const override { return Patterns::window(); }

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
