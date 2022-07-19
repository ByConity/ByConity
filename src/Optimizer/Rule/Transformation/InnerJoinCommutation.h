#pragma once

#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
/**
 * InnerJoinCommutation is mutually exclusive with JoinEnumOnGraph rule,
 * the later also do inner join commutation works.
 *
 * Transforms:
 * <pre>
 * - Inner Join
 *     - X
 *     - Y
 * </pre>
 * Into:
 * <pre>
 * - Inner Join
 *     - Y
 *     - X
 * </pre>
 */
class InnerJoinCommutation : public Rule
{
public:
    RuleType getType() const override { return RuleType::INNER_JOIN_COMMUTATION; }
    String getName() const override { return "INNER_JOIN_COMMUTATION"; }

    PatternPtr getPattern() const override;

    const std::vector<RuleType> & blockRules() const override;

    static bool supportSwap(const JoinStep & s) { return s.getKind() == ASTTableJoin::Kind::Inner && s.supportSwap(); }
    static PlanNodePtr swap(JoinNode & node, RuleContext & rule_context);

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
