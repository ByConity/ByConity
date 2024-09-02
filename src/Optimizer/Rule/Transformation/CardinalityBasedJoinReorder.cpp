#include <Optimizer/Rule/Transformation/CardinalityBasedJoinReorder.h>

#include <Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/MultiJoinStep.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace DB
{
const std::vector<RuleType> & CardinalityBasedJoinReorder::blockRules() const
{
    static std::vector<RuleType> block{RuleType::JOIN_ENUM_ON_GRAPH, RuleType::CARDILALITY_BASED_JOIN_REORDER};
    return block;
}

ConstRefPatternPtr CardinalityBasedJoinReorder::getPattern() const
{
    /* can't make static lambda with capture */
    return pattern;
}

struct InterJoinNodeInfo
{
    UInt64 row_count;
    PlanNodePtr join_node;
    GroupId left_child_group_id;
    GroupId right_child_group_id;

    bool operator<(const InterJoinNodeInfo & rhs) const
    {
        return std::make_tuple(row_count, join_node->getId(), left_child_group_id, right_child_group_id) < std::make_tuple(rhs.row_count, rhs.join_node->getId(), rhs.left_child_group_id, rhs.right_child_group_id);
    }
};

TransformResult CardinalityBasedJoinReorder::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * multi_join_node = dynamic_cast<MultiJoinNode *>(node.get());
    if (!multi_join_node|| !rule_context.optimization_context->getMemo().getGroupById(rule_context.group_id)->isJoinRoot())
        return {};

    auto multi_join_step = multi_join_node->getStep();
    const auto & graph = multi_join_step->getGraph();
    if (graph.getNodes().size() < 2)
        return {};

    std::vector<String> output_symbols;
    for (const auto & column : multi_join_step->getOutputStream().header)
    {
        output_symbols.emplace_back(column.name);
    }

    const auto & memo = rule_context.optimization_context->getMemo();

    std::vector<std::pair<UInt64, GroupId>> ordered_base_nodes;
    for (const auto & group_id : graph.getNodes())
    {
        PlanNodeStatisticsPtr stat = memo.getGroupById(group_id)->getStatistics().value_or(nullptr);
        if (!stat)
            return {};
        ordered_base_nodes.emplace_back(stat->getRowCount(), group_id);
    }
    std::sort(
        ordered_base_nodes.begin(),
        ordered_base_nodes.end(),
        [](const std::pair<UInt64, GroupId> & lhs, const std::pair<UInt64, GroupId> & rhs) { return lhs < rhs; });

    PlanNodes results;

    int k = std::min(rule_context.context->getSettingsRef().heuristic_join_reorder_enumeration_times.value, ordered_base_nodes.size());

    // heuristic enumerate k times.
    for (int i = 0; i < k; i++)
    {
        GroupIdToIds group_id_map;
        for (const auto & group_id : graph.getNodes())
            group_id_map[group_id].insert(group_id);

        GroupId current_group_id;
        std::unordered_set<GroupId> remaining_base_nodes(graph.getNodes().begin(), graph.getNodes().end());

        const auto & min_cardinality_base_node = ordered_base_nodes.at(i);
        current_group_id = min_cardinality_base_node.second;
        remaining_base_nodes.erase(min_cardinality_base_node.second);

        while (true)
        {
            std::vector<InterJoinNodeInfo> inter_join_nodes;

            bool is_final_join = remaining_base_nodes.size() == 1;

            for (const auto & group_id : remaining_base_nodes)
            {
                PlanNodePtr new_join_node = JoinReorderUtils::createNewJoin(current_group_id, group_id, group_id_map, graph, rule_context);
                if (new_join_node == nullptr)
                    continue;

                const auto & join_step = static_cast<const JoinStep &>(*new_join_node->getStep());

                UInt64 row_count = 0;
                if (!is_final_join)
                {
                    auto left_stat = memo.getGroupById(current_group_id)->getStatistics().value_or(nullptr);
                    auto right_stat = memo.getGroupById(group_id)->getStatistics().value_or(nullptr);
                    if (!left_stat || !right_stat)
                        return {};


                    auto stat = JoinEstimator::computeCardinality(
                        *left_stat,
                        *right_stat,
                        join_step.getLeftKeys(),
                        join_step.getRightKeys(),
                        join_step.getKind(),
                        join_step.getStrictness(),
                        *rule_context.context,
                        memo.getGroupById(current_group_id)->isTableScan(),
                        memo.getGroupById(group_id)->isTableScan(),
                        {JoinReorderUtils::computeFilterSelectivity(current_group_id, memo),
                         JoinReorderUtils::computeFilterSelectivity(group_id, memo)},
                        {},
                        true);
                    if (!stat)
                        return {};
                    row_count = stat->getRowCount();
                }

                inter_join_nodes.emplace_back(InterJoinNodeInfo{row_count, new_join_node, current_group_id, group_id});
            }
            
            std::sort(inter_join_nodes.begin(), inter_join_nodes.end());

            const auto & min_join_node = inter_join_nodes.at(0);
            if (!is_final_join) // record inter join into group.
            {
                GroupExprPtr join_expr;
                rule_context.optimization_context->getOptimizerContext().recordPlanNodeIntoGroup(
                    min_join_node.join_node, join_expr, RuleType::CARDILALITY_BASED_JOIN_REORDER);
                auto new_group_id = join_expr->getGroupId();
                for (auto type : blockRules())
                    join_expr->setRuleExplored(type);

                for (const auto & left_group_id : group_id_map.at(min_join_node.left_child_group_id))
                    group_id_map[new_group_id].insert(left_group_id);
                for (const auto & right_group_id : group_id_map.at(min_join_node.right_child_group_id))
                    group_id_map[new_group_id].insert(right_group_id);

                current_group_id = new_group_id;
                remaining_base_nodes.erase(min_join_node.right_child_group_id);
            }
            else // just return the final join. don't record.
            {
                auto join_order = min_join_node.join_node;
                JoinReorderUtils::pruneJoinColumns(output_symbols, join_order, rule_context.context);
                results.emplace_back(join_order);
                break;
            }
        }
    }

    return TransformResult{results};
}

}
