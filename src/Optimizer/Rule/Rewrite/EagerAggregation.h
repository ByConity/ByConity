#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{

// only Eager Group-By at present.
class EagerAggregation : public Rule
{
public:
    RuleType getType() const override { return RuleType::EAGER_AGGREGATION; }
    String getName() const override { return "EAGER_AGGREGATION"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_eager_aggregation; }    
    ConstRefPatternPtr getPattern() const override;

    bool excludeIfTransformSuccess() const override { return true; }
    bool excludeIfTransformFailure() const override { return true; }

    const std::vector<RuleType> & blockRules() const override;
    TransformResult transformImpl(PlanNodePtr aggregation, const Captures & captures, RuleContext & rule_context) override;
};

struct LocalGroupByTarget
{
    PlanNodePtr bottom_join;
    int bottom_join_child_index;
    AggregateDescriptions aggs;
    Names keys;
    int join_layer; // how many join is influenced.
    bool push_through_final_projection = false;
};

using LocalGroupByTargetMap = std::unordered_multimap<PlanNodeId, LocalGroupByTarget>;

[[maybe_unused]]static String formatS0(const AggregateDescriptions & s0)
{
    String strb;
    for (const auto & agg : s0)
    {
        strb += "`" + agg.column_name + "`(";
        strb += fmt::format("{}", fmt::join(agg.argument_names, ","));
        strb += ")";
    }
    return strb;
}

}
