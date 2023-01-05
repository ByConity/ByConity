#pragma once

#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
class LeftJoinToRightJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::LEFT_JOIN_TO_RIGHT_JOIN; }
    String getName() const override { return "LEFT_JOIN_TO_RIGHT_JOIN"; }

    PatternPtr getPattern() const override;

    // Left join with filter is not allowed convert to Right join with filter. (nest loop join only support left join).
    static bool supportSwap(const JoinStep & s)
    {
        return s.getKind() == ASTTableJoin::Kind::Left && s.supportSwap() && PredicateUtils::isTruePredicate(s.getFilter());
    }

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
