#include <QueryPlan/Hints/ImplementJoinAlgorithmHints.h>
#include <QueryPlan/Hints/JoinAlgorithmHints.h>
#include <common/logger_useful.h>


namespace DB
{

bool ImplementJoinAlgorithmHints::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    JoinAlgorithmHintsVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    VisitorUtil::accept(*plan.getPlanNode(), visitor, v);
    return true;
}

String ImplementJoinAlgorithmHints::name() const
{
    return "ImplementJoinAlgorithmHints";
}

HintsStringSet JoinAlgorithmHintsVisitor::visitJoinNode(JoinNode & node, Void & v)
{
    HintsStringSet left_hints_set = VisitorUtil::accept(node.getChildren()[0], *this, v);
    HintsStringSet right_hints_set = VisitorUtil::accept(node.getChildren()[1], *this, v);

    if (left_hints_set.empty())
        return right_hints_set;
    else if (right_hints_set.empty())
        return left_hints_set;

    HintsStringSet common_hints;
    for (const auto & left_hint : left_hints_set)
    {
        if (right_hints_set.count(left_hint))
            common_hints.insert(left_hint);
    }

    if (common_hints.empty())
    {
        left_hints_set.insert(right_hints_set.begin(), right_hints_set.end());
        return left_hints_set;
    }

    JoinAlgorithmType relation_type = JoinAlgorithmType::UNKNOWN;
    for (const auto & hint_str : common_hints)
    {
        if (hint_str.starts_with("USE_GRACE_HASH"))
        {
            relation_type = JoinAlgorithmType::USE_GRACE_HASH;
            break;
        }
    }

    auto & step = *node.getStep();
    if (relation_type == JoinAlgorithmType::USE_GRACE_HASH && step.getJoinAlgorithm() == JoinAlgorithm::AUTO)
        step.setJoinAlgorithm(JoinAlgorithm::GRACE_HASH);

    left_hints_set.insert(right_hints_set.begin(), right_hints_set.end());
    return left_hints_set;
}

HintsStringSet JoinAlgorithmHintsVisitor::visitPlanNode(PlanNodeBase & node, Void & v)
{
    HintsStringSet hints_set;
    for (auto & child : node.getChildren())
    {
        auto child_hints_set = VisitorUtil::accept(*child, *this, v);
        hints_set.insert(child_hints_set.begin(), child_hints_set.end());
    }
    return hints_set;
}

HintsStringSet JoinAlgorithmHintsVisitor::visitCTERefNode(CTERefNode & node, Void & v)
{
    CTEId cte_id = node.getStep()->getId();
    cte_helper.accept(cte_id, *this, v);
    return {};
}

HintsStringSet JoinAlgorithmHintsVisitor::visitTableScanNode(TableScanNode & node, Void &)
{
    const auto & hints = node.getStep()->getHints();
    HintsStringSet hints_set;
    for (const auto & hint : hints)
    {
        if (hint->getType() == HintCategory::JOIN_ALGORITHM)
            hints_set.insert(fmt::format("{}({})", hint->getName(), boost::algorithm::join(hint->getOptions(), ", ")));
    }
    return hints_set;
}

}
