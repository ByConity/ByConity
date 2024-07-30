#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/MultiJoinStep.h>

#include <Optimizer/Rule/Rule.h>
#include <Optimizer/Rule/Transformation/JoinReorderUtils.h>

#include <Optimizer/Rule/Patterns.h>
#include <utility>

namespace DB
{
class SelectivityBasedJoinReorder : public Rule
{
public:
    explicit SelectivityBasedJoinReorder(size_t max_join_size_): max_join_size(max_join_size_) {
        pattern = Patterns::multiJoin()
            .matchingStep<MultiJoinStep>([&](const MultiJoinStep & s) { return s.getGraph().getNodes().size() > max_join_size; })
            .result();
    }
    RuleType getType() const override { return RuleType::SELECTIVITY_BASED_JOIN_REORDER; }
    String getName() const override { return "SELECTIVITY_BASED_JOIN_REORDER"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_selectivity_based_join_reorder; }

    const std::vector<RuleType> & blockRules() const override;
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

private:
    static PlanNodePtr createNewJoin(GroupId left_id, GroupId right_id, const GroupIdToIds & group_id_map, const Graph & graph, RuleContext & rule_context);
    PlanNodePtr getJoinOrder(const Graph & graph, RuleContext & rule_context);

    size_t max_join_size;
    PatternPtr pattern;
};

struct EdgeSelectivity
{
    GroupId left_id;
    GroupId right_id;
    String left_symbol;
    String right_symbol;
    double selectivity;
    size_t output;
    size_t min_input;
};

struct EdgeSelectivityCompare
{
    bool operator()(const EdgeSelectivity & a, const EdgeSelectivity & b)
    {
        double a_s = a.selectivity > 0.98 ? 1 : a.selectivity;
        double b_s = b.selectivity > 0.98 ? 1 : b.selectivity;
        if (std::fabs(a_s - b_s) >= 1e-7)
            return a_s > b_s;

        if (a.output != b.output)
            return a.output > b.output;

        if (a.min_input != b.min_input)
            return a.min_input > b.min_input;

        if (a.left_symbol != b.left_symbol)
            return a.left_symbol < b.left_symbol;
        
        if (a.right_symbol != b.right_symbol)
            return a.right_symbol < b.right_symbol;

        return std::make_tuple(a.left_id, a.right_id) < std::make_tuple(b.left_id, b.right_id);
    }
};
}
