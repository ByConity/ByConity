#pragma once
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{
class FilterWindowToPartitionTopN : public Rule
{
public:
    RuleType getType() const override { return RuleType::FILTER_WINDOW_TO_PARTITION_TOPN; }
    String getName() const override { return "FILTER_WINDOW_TO_PARTITION_TOPN"; }

    PatternPtr getPattern() const override { return Patterns::filter()->withSingle(Patterns::window()->withSingle(Patterns::exchange())); }

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
