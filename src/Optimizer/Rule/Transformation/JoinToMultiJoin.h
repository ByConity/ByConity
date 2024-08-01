#pragma once
#include <Functions/FunctionsHashing.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>
#include <boost/dynamic_bitset.hpp>

#include <unordered_set>
#include <utility>

namespace DB
{
using GroupId = UInt32;
class CascadesContext;
class JoinToMultiJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::JOIN_TO_MULTI_JOIN; }
    String getName() const override { return "JOIN_TO_MULTI_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_join_to_multi_join; }
    ConstRefPatternPtr getPattern() const override;
    static bool isSupport(const JoinStep & s) { return s.supportReorder(true) && !s.isSimpleReordered() && !s.isOrdered(); }

    static PlanNodes createMultiJoin(
        ContextMutablePtr context,
        CascadesContext & optimizer_context,
        const JoinStep * join_step,
        GroupId group_id,
        GroupId left_group_id,
        GroupId right_group_id);

private:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


}
