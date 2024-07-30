#pragma once

#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>
#include <Optimizer/Rule/Transformation/JoinReorderUtils.h>
#include <Optimizer/Rule/Patterns.h>

namespace DB
{
/**
 * CardinalityBasedJoinReorder is a greedy join reorder algorithm.
 *
 * A total of k rounds of iteration are carried out, k = the number of base nodes.
 * At the beginning, we heuristically select the base node with the smallest cardinality.
 * In each round of iteration, candidate which `after join cardinality` is the smallest is selected to join,
 * so that the size of the remaining base nodes is reduced by one.
 * Finally, the remaining base nodes is reduced to zero, and current join is the result.
 */

class CardinalityBasedJoinReorder : public Rule
{
public:
    explicit CardinalityBasedJoinReorder(size_t max_join_size_): max_join_size(max_join_size_) {
        pattern = Patterns::multiJoin()
            .matchingStep<MultiJoinStep>([&](const MultiJoinStep & s) { return s.getGraph().getNodes().size() > max_join_size; })
            .result();
    }
    RuleType getType() const override { return RuleType::CARDILALITY_BASED_JOIN_REORDER; }
    String getName() const override { return "CARDILALITY_BASED_JOIN_REORDER"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_cardinality_based_join_reorder; }

    const std::vector<RuleType> & blockRules() const override;
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    size_t max_join_size;
    PatternPtr pattern;
};

}
