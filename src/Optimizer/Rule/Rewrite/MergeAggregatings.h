#pragma once
#include <Optimizer/Rule/Rule.h>

#include <Optimizer/ExpressionRewriter.h>

namespace DB
{

class MergeAggregatings : public Rule
{
public:
    RuleType getType() const override { return RuleType::MERGE_AGGREGATINGS; }
    String getName() const override { return "MERGE_AGGREGATINGS"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
