#include <Optimizer/Rule/Transformation/JoinToMultiJoin.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/Task.h>
#include <Optimizer/Graph.h>
#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/MultiJoinStep.h>

namespace DB
{
ConstRefPatternPtr JoinToMultiJoin::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) { return isSupport(s); })
        .with(Patterns::tree(), Patterns::tree())
        .result();
    return pattern;
}

PlanNodes JoinToMultiJoin::createMultiJoin(
    ContextMutablePtr context,
    CascadesContext &  optimizer_context,
    const JoinStep * join_step,
    GroupId group_id,
    GroupId left_group_id,
    GroupId right_group_id)
{
    auto group = optimizer_context.getMemo().getGroupById(group_id);
    auto left_group = optimizer_context.getMemo().getGroupById(left_group_id);
    auto right_group = optimizer_context.getMemo().getGroupById(right_group_id);

    PlanNodes result;
    for (const auto & left_join_set : left_group->getJoinSets())
    {
        for (const auto & right_join_set : right_group->getJoinSets())
        {
            std::vector<ConstASTPtr> conjuncts;
            if (join_step->getFilter() && !PredicateUtils::isTruePredicate(join_step->getFilter()))
            {
                conjuncts.emplace_back(join_step->getFilter());
            }
            if (left_join_set.getFilter() && !PredicateUtils::isTruePredicate(left_join_set.getFilter()))
            {
                conjuncts.emplace_back(left_join_set.getFilter());
            }
            if (right_join_set.getFilter() && !PredicateUtils::isTruePredicate(right_join_set.getFilter()))
            {
                conjuncts.emplace_back(right_join_set.getFilter());
            }


            JoinSet merged_join_set{
                left_join_set,
                right_join_set,
                join_step->getLeftKeys(),
                join_step->getRightKeys(),
                PredicateUtils::combineConjuncts(conjuncts)};

            if (!group->containsJoinSet(merged_join_set))
            {
                group->addJoinSet(merged_join_set);
                auto multi_join_step = std::make_shared<MultiJoinStep>(
                    join_step->getOutputStream(),
                    Graph::build(
                        merged_join_set.getGroups(),
                        merged_join_set.getUnionFind(),
                        merged_join_set.getFilter(),
                        optimizer_context.getMemo()));

                PlanNodes input_children;
                std::vector<GroupId> children_groups;
                for (const auto & inner_group_id : multi_join_step->getGraph().getNodes())
                {
                    auto leaf_node = optimizer_context.getMemo().getGroupById(inner_group_id)->createLeafNode(context);
                    children_groups.emplace_back(inner_group_id);
                    input_children.emplace_back(leaf_node);
                }

                auto multi_join_node = MultiJoinNode::createPlanNode(context->nextNodeId(), std::move(multi_join_step), input_children);
                result.emplace_back(multi_join_node);
            }
        }
    }
    return result;
}

TransformResult JoinToMultiJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto group_id = context.group_id;

    auto left_group_id = dynamic_cast<const AnyStep *>(node->getChildren()[0]->getStep().get())->getGroupId();
    auto right_group_id = dynamic_cast<const AnyStep *>(node->getChildren()[1]->getStep().get())->getGroupId();

    const auto * join_step = dynamic_cast<const JoinStep *>(node->getStep().get());

    return TransformResult{
        createMultiJoin(context.context, context.optimization_context->getOptimizerContext(), join_step, group_id, left_group_id, right_group_id)};
}


}
