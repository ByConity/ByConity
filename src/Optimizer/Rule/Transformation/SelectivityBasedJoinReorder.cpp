#include <Optimizer/Rule/Transformation/SelectivityBasedJoinReorder.h>

#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/Rule/Transformation/JoinReorderUtils.h>
#include <Optimizer/SymbolUtils.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/PlanPattern.h>
#include <QueryPlan/ProjectionStep.h>
#include <Optimizer/JoinOrderUtils.h>

namespace DB
{
const std::vector<RuleType> & SelectivityBasedJoinReorder::blockRules() const
{
    static std::vector<RuleType> block{RuleType::JOIN_ENUM_ON_GRAPH, RuleType::SELECTIVITY_BASED_JOIN_REORDER};
    return block;
}

ConstRefPatternPtr SelectivityBasedJoinReorder::getPattern() const
{
    /* can't make static lambda with capture */
    return pattern;
}

TransformResult SelectivityBasedJoinReorder::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto * multi_join_node = dynamic_cast<MultiJoinNode *>(node.get());
    if (!multi_join_node || !rule_context.optimization_context->getMemo().getGroupById(rule_context.group_id)->isJoinRoot())
        return {};

    // LOG_DEBUG(rule_context.optimization_context->getOptimizerContext().getLog(), "SelectivityBasedJoinReorder");

    auto multi_join_step = multi_join_node->getStep();
    const auto & graph = multi_join_step->getGraph();
    if (graph.getNodes().size() < 2)
        return {};

    std::vector<String> output_symbols;
    for (const auto & column : multi_join_step->getOutputStream().header)
    {
        output_symbols.emplace_back(column.name);
    }

    auto join_order = getJoinOrder(graph, rule_context);

    if (join_order)
    {
        JoinReorderUtils::pruneJoinColumns(output_symbols, join_order, rule_context.context);
        return TransformResult{join_order};
    }

    return TransformResult{};
}

PlanNodePtr SelectivityBasedJoinReorder::getJoinOrder(const Graph & graph, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto & memo = rule_context.optimization_context->getMemo();

    std::priority_queue<EdgeSelectivity, std::vector<EdgeSelectivity>, EdgeSelectivityCompare> selectivities;
    for (const auto & item : graph.getEdges())
    {
        auto left_group_id = item.first;
        auto is_left_base_table = memo.getGroupById(left_group_id)->isTableScan();
        auto left_stats = memo.getGroupById(left_group_id)->getStatistics().value_or(nullptr);
        if (!left_stats)
            return {};
        double left_selectivity = JoinReorderUtils::computeFilterSelectivity(left_group_id, memo);

        for (const auto & [right_group_id, edges] : item.second)
        {
            auto is_right_base_table = memo.getGroupById(right_group_id)->isTableScan();
            auto right_stats = memo.getGroupById(right_group_id)->getStatistics().value_or(nullptr);
            if (!right_stats)
                return {};
            double right_selectivity = JoinReorderUtils::computeFilterSelectivity(right_group_id, memo);

            for (const auto & edge : edges)
            {
                // because join graph is undirected graph, only use the same edge one time.
                Names left_keys = {edge.source_symbol};
                Names right_keys = {edge.target_symbol};
                if (left_group_id < right_group_id)
                {
                    auto join_stats = JoinEstimator::computeCardinality(
                        *left_stats,
                        *right_stats,
                        left_keys,
                        right_keys,
                        ASTTableJoin::Kind::Inner,
                        ASTTableJoin::Strictness::All,
                        *context,
                        is_left_base_table,
                        is_right_base_table,
                        {left_selectivity, right_selectivity},
                        {},
                        true);

                    if (!join_stats)
                        return {};

                    size_t join_card = join_stats->getRowCount();

                    selectivities.push(EdgeSelectivity{
                        left_group_id,
                        right_group_id,
                        edge.source_symbol,
                        edge.target_symbol,
                        static_cast<double>(join_card)
                            / static_cast<double>(std::max(left_stats->getRowCount(), right_stats->getRowCount())),
                        join_card,
                        std::min(left_stats->getRowCount(), right_stats->getRowCount())});
                }
            }
        }
    }

    std::unordered_map<GroupId, GroupId> id_to_root_group_id;
    for (const auto & group_id : graph.getNodes())
    {
        id_to_root_group_id[group_id] = group_id;
    }

    GroupIdToIds id_to_source_tables;
    for (const auto & group_id : graph.getNodes())
    {
        id_to_source_tables[group_id].insert(group_id);
    }

    PlanNodePtr result;

    size_t new_join_cnt = 0;
    const auto & edges = graph.getEdges();
    while (!selectivities.empty())
    {
        auto selectivity = selectivities.top();
        selectivities.pop();
        auto left_group_id = id_to_root_group_id.at(selectivity.left_id);
        auto right_group_id = id_to_root_group_id.at(selectivity.right_id);
        if (left_group_id != right_group_id)
        {
            auto & left_tables = id_to_source_tables[left_group_id];
            auto & right_tables = id_to_source_tables[right_group_id];

            auto new_join_node = JoinReorderUtils::createNewJoin(left_group_id, right_group_id, id_to_source_tables, graph, rule_context);

            if (!new_join_node)
                return nullptr; // it's unreachable in general.

                        GroupId new_group_id = 0;
            ++new_join_cnt;
            if (new_join_cnt < graph.getNodes().size() - 1)
            {
                GroupExprPtr join_expr;
                rule_context.optimization_context->getOptimizerContext().recordPlanNodeIntoGroup(
                    new_join_node, join_expr, RuleType::SELECTIVITY_BASED_JOIN_REORDER);
                new_group_id = join_expr->getGroupId();
                                for (auto type : blockRules())
                    join_expr->setRuleExplored(type);
            }

            id_to_root_group_id[left_group_id] = new_group_id;
            id_to_root_group_id[right_group_id] = new_group_id;
            id_to_root_group_id[new_group_id] = new_group_id;

            id_to_source_tables[new_group_id].insert(left_group_id);
            id_to_source_tables[new_group_id].insert(right_group_id);
            id_to_source_tables[new_group_id].insert(new_group_id);
            for (auto left_table : left_tables)
            {
                id_to_root_group_id[left_table] = new_group_id;
                id_to_source_tables[new_group_id].insert(left_table);
            }
            for (auto right_table : right_tables)
            {
                id_to_root_group_id[right_table] = new_group_id;
                id_to_source_tables[new_group_id].insert(right_table);
            }
            result = new_join_node;

            // add the selectivity of (new join node, other waiting nodes) to queue
            const auto & source_tables = id_to_source_tables.at(new_group_id);
            std::unordered_set<GroupId> outer_waiting_nodes_set;
            std::unordered_map<GroupId, std::vector<Graph::Edge>> outer_edges;
            for (const auto source_group_id : source_tables)
            {
                if (edges.contains(source_group_id))
                {
                    for (const auto & [target_group_id, edges] : edges.at(source_group_id))
                    {
                        if (!source_tables.contains(target_group_id))
                        {
                            outer_waiting_nodes_set.emplace(id_to_root_group_id.at(target_group_id));
                            for (const auto & edge : edges)
                                outer_edges[id_to_root_group_id.at(target_group_id)].emplace_back(edge);
                        }
                    }
                }
            }

            std::vector<PlanNodeId> outer_waiting_nodes{outer_waiting_nodes_set.begin(), outer_waiting_nodes_set.end()};
            std::sort(outer_waiting_nodes.begin(), outer_waiting_nodes.end());

            for (const auto & outer_right_group_id : outer_waiting_nodes)
            {
                Utils::checkState(id_to_root_group_id.at(new_group_id) == new_group_id);
                auto outer_left_group_id = new_group_id;
                auto is_left_base_table = memo.getGroupById(outer_left_group_id)->isTableScan();
                auto left_stats = memo.getGroupById(outer_left_group_id)->getStatistics().value_or(nullptr);
                if (!left_stats)
                    return {};
                double left_selectivity = JoinReorderUtils::computeFilterSelectivity(left_group_id, memo);

                auto is_right_base_table = memo.getGroupById(outer_right_group_id)->isTableScan();
                auto right_stats = memo.getGroupById(outer_right_group_id)->getStatistics().value_or(nullptr);
                if (!right_stats)
                    return {};
                double right_selectivity = JoinReorderUtils::computeFilterSelectivity(right_group_id, memo);

                // because join graph is undirected graph, only use the same edge one time.
                Names new_left_keys;
                Names new_right_keys;
                for (const auto & edge : outer_edges[outer_right_group_id])
                {
                    new_left_keys.emplace_back(edge.source_symbol);
                    new_right_keys.emplace_back(edge.target_symbol);
                }
                auto join_stats = JoinEstimator::computeCardinality(
                    *left_stats,
                    *right_stats,
                    new_left_keys,
                    new_right_keys,
                    ASTTableJoin::Kind::Inner,
                    ASTTableJoin::Strictness::All,
                    *context,
                    is_left_base_table,
                    is_right_base_table,
                    {left_selectivity, right_selectivity},
                    {},
                    true);
                if (!join_stats)
                    return {};

                size_t join_card = join_stats->getRowCount();

                selectivities.push(EdgeSelectivity{
                    outer_left_group_id,
                    outer_right_group_id,
                    new_left_keys[0],
                    new_right_keys[0],
                    static_cast<double>(join_card) / static_cast<double>(std::max(left_stats->getRowCount(), right_stats->getRowCount())),
                    join_card,
                    std::min(left_stats->getRowCount(), right_stats->getRowCount())});
            }
        }
    }

    return result;
}

}
