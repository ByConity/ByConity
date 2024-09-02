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
class InnerJoinAssociate : public Rule
{
public:
    RuleType getType() const override { return RuleType::INNER_JOIN_ASSOCIATE; }
    String getName() const override { return "INNER_JOIN_ASSOCIATE"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_inner_join_associate; }
    ConstRefPatternPtr getPattern() const override;

    //fixme@kaixi: remove this
    const std::vector<RuleType> & blockRules() const override;

    static bool supportSwap(const JoinStep & s) { return s.getKind() == ASTTableJoin::Kind::Inner && s.supportSwap(); }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
