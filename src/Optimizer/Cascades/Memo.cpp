#include <Optimizer/Cascades/Memo.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <QueryPlan/AnyStep.h>

namespace DB
{
GroupExprPtr Memo::insertGroupExpr(GroupExprPtr group_expr, CascadesContext & context, GroupId target)
{
    // If leaf, then just return
    if (group_expr->getStep()->getType() == IQueryPlanStep::Type::Any)
    {
        const auto * leaf = dynamic_cast<const AnyStep *>(group_expr->getStep().get());
        group_expr->setGroupId(leaf->getGroupId());
        return nullptr;
    }

    if (group_expr->getStep()->getType() == IQueryPlanStep::Type::Join)
    {
        const auto * join = dynamic_cast<const JoinStep *>(group_expr->getStep().get());
        if (join->isMagic())
        {
            groups[group_expr->getChildrenGroups()[1]]->setMagic(true);
        }
    }

    group_expr->setGroupId(target);
    // Lookup in hash table
    auto it = group_expressions.find(group_expr);
    // duplicate group expression
    if (it != group_expressions.end())
    {
        return *it;
    }

    group_expressions.insert(group_expr);

    // New expression, so try to insert into an existing group or
    // create a new group if none specified
    GroupId group_id;
    if (target == UNDEFINED_GROUP)
    {
        group_id = addNewGroup();
        // LOG_DEBUG(context.getLog(), "New Group Id " << group_id << "; Rule Type: "
        //                                             << static_cast<int>(group_expr->getProducerRule()));
    }
    else
    {
        group_id = target;
    }

    auto group = getGroupById(group_id);
    group->addExpression(group_expr, context);
    return group_expr;
}


}
