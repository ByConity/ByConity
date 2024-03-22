/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/Rewriter/SimpleReorderJoin.h>

#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolUtils.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/PlanPattern.h>
#include <QueryPlan/ProjectionStep.h>


namespace DB
{
void SimpleReorderJoin::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    if (context->getSettingsRef().heuristic_join_reorder_enumeration_times != 10)
        return;
    if (!context->getSettingsRef().enable_join_reorder)
        return;

    auto join_size = PlanPattern::maxJoinSize(plan, context);
    if (join_size <= context->getSettingsRef().max_graph_reorder_size)
        return;

    SimpleReorderJoinVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr SimpleReorderJoinVisitor::visitJoinNode(JoinNode & node, Void & v)
{
    if (node.getStep()->isOrdered())
        return node.shared_from_this();

    auto stats = CardinalityEstimator::estimate(node, cte_info, context);
    if (!stats || reordered.contains(node.getId()))
        return visitPlanNode(node, v);

    auto join_ptr = node.shared_from_this();
    JoinGraph join_graph = JoinGraph::build(join_ptr, context, false, true, false, false);
    if (join_graph.size() < 2 || join_graph.size() <= context->getSettingsRef().max_graph_reorder_size)
        return visitPlanNode(node, v);

    auto join_order = getJoinOrder(join_graph);

    if (join_order)
    {
        std::vector<String> output_symbols;
        for (const auto & column : node.getStep()->getOutputStream().header)
        {
            output_symbols.emplace_back(column.name);
        }

        join_ptr = buildJoinTree(output_symbols, join_graph, join_order, context);
        for (const auto & id : join_graph.getNodes())
        {
            reordered.insert(id->getId());
        }
    }

    join_ptr = visitPlanNode(*join_ptr, v);
    return join_ptr;
}

PlanNodePtr SimpleReorderJoinVisitor::getJoinOrder(JoinGraph & graph)
{
    std::unordered_map<PlanNodeId, PlanNodePtr> id_to_node;
    for (const auto & node : graph.getNodes())
    {
        id_to_node[node->getId()] = node;
    }

    std::priority_queue<EdgeSelectivity, std::vector<EdgeSelectivity>, EdgeSelectivityCompare> selectivities;
    for (const auto & item : graph.getEdges())
    {
        auto left_id = item.first;
        auto left_node = id_to_node[left_id];
        auto left_base_table = left_node->getStep()->getType() == IQueryPlanStep::Type::TableScan;
        auto left_stats = CardinalityEstimator::estimate(*left_node, cte_info, context);
        if (!left_stats)
            return {};

        for (const auto & edge : item.second)
        {
            const auto & right_node = edge.getTargetNode();
            auto right_base_table = right_node->getStep()->getType() == IQueryPlanStep::Type::TableScan;
            auto right_id = edge.getTargetNode()->getId();
            auto right_stats = CardinalityEstimator::estimate(*right_node, cte_info, context);
            if (!right_stats)
                return {};

            // because join graph is undirected graph, only use the same edge one time.
            Names left_keys = {edge.getSourceSymbol()};
            Names right_keys = {edge.getTargetSymbol()};
            if (left_id < right_id)
            {
                auto join_stats = JoinEstimator::computeCardinality(
                    *left_stats.value(),
                    *right_stats.value(),
                    left_keys,
                    right_keys,
                    ASTTableJoin::Kind::Inner,
                    ASTTableJoin::Strictness::All,
                    *context,
                    // todo is base table
                    left_base_table,
                    right_base_table);

                if (!join_stats)
                    return {};

                size_t join_card = join_stats->getRowCount();

                selectivities.push(EdgeSelectivity{
                    left_id,
                    right_id,
                    edge.getSourceSymbol(),
                    edge.getTargetSymbol(),
                    static_cast<double>(join_card)
                        / static_cast<double>(std::max(left_stats.value()->getRowCount(), right_stats.value()->getRowCount())),
                    join_card,
                    std::min(left_stats.value()->getRowCount(), right_stats.value()->getRowCount())});
            }
        }
    }

    std::unordered_map<PlanNodeId, PlanNodePtr> id_to_join_node;
    for (const auto & node : graph.getNodes())
    {
        id_to_join_node[node->getId()] = node;
    }

    std::unordered_map<PlanNodeId, std::unordered_set<PlanNodeId>> id_to_source_tables;
    for (const auto & node : graph.getNodes())
    {
        id_to_source_tables[node->getId()].insert(node->getId());
    }

    PlanNodePtr result;
    std::set<PlanNodeId> joined_nodes;
    const auto & edges = graph.getEdges();
    
    while (!selectivities.empty())
    {
        auto selectivity = selectivities.top();
        selectivities.pop();
        auto left_join_node = id_to_join_node[selectivity.left_id];
        auto right_join_node = id_to_join_node[selectivity.right_id];
        auto left_join_node_id = left_join_node->getId();
        auto right_join_node_id = right_join_node->getId();
        if (left_join_node_id != right_join_node_id)
        {
            auto & left_tables = id_to_source_tables[left_join_node_id];
            auto & right_tables = id_to_source_tables[right_join_node_id];
            Names left_keys;
            Names right_keys;
            for (const auto & left_table_id : left_tables)
            {
                if (edges.contains(left_table_id))
                {
                    for (const auto & edge : edges.at(left_table_id))
                    {
                        if (right_tables.contains(edge.getTargetNode()->getId()))
                        {
                            left_keys.emplace_back(edge.getSourceSymbol());
                            right_keys.emplace_back(edge.getTargetSymbol());
                        }
                    }
                }
            }


            const DataStream & left_data_stream = left_join_node->getStep()->getOutputStream();
            const DataStream & right_data_stream = right_join_node->getStep()->getOutputStream();
            DataStreams streams = {left_data_stream, right_data_stream};

            auto left_header = left_data_stream.header;
            auto right_header = right_data_stream.header;
            NamesAndTypes output;
            for (const auto & item : left_header)
            {
                output.emplace_back(NameAndTypePair{item.name, item.type});
            }
            for (const auto & item : right_header)
            {
                output.emplace_back(NameAndTypePair{item.name, item.type});
            }

            auto new_join_step = std::make_shared<JoinStep>(
                streams,
                DataStream{.header = output},
                ASTTableJoin::Kind::Inner,
                ASTTableJoin::Strictness::All,
                context->getSettingsRef().max_threads,
                context->getSettingsRef().optimize_read_in_order,
                left_keys,
                right_keys);
            new_join_step->setSimpleReordered(true);
            
            auto new_join_node
                = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(new_join_step), PlanNodes{left_join_node, right_join_node});

            id_to_join_node[left_join_node_id] = new_join_node;
            id_to_source_tables[new_join_node->getId()].insert(left_join_node_id);
            id_to_join_node[right_join_node_id] = new_join_node;
            id_to_source_tables[new_join_node->getId()].insert(right_join_node_id);
            id_to_join_node[new_join_node->getId()] = new_join_node;
            id_to_source_tables[new_join_node->getId()].insert(new_join_node->getId());
            for (auto left_table : left_tables)
            {
                id_to_join_node[left_table] = new_join_node;
                id_to_source_tables[new_join_node->getId()].insert(left_table);
            }
            for (auto right_table : right_tables)
            {
                id_to_join_node[right_table] = new_join_node;
                id_to_source_tables[new_join_node->getId()].insert(right_table);
            }
            result = new_join_node;
            reordered.insert(result->getId());

            // add the selectivity of (new join node, other waiting nodes) to queue

            const auto & source_tables = id_to_source_tables.at(new_join_node->getId());
            std::unordered_set<PlanNodeId> outer_waiting_nodes_set;
            std::unordered_map<PlanNodeId, std::vector<Edge>> outer_edges;
            for (const auto table : source_tables)
            {
                if (edges.contains(table))
                {
                    for (const auto & edge : edges.at(table))
                    {
                        if (!source_tables.contains(edge.getTargetNode()->getId()))
                        {
                            auto target_id = edge.getTargetNode()->getId();
                            outer_edges[id_to_join_node.at(target_id)->getId()].emplace_back(edge);
                            outer_waiting_nodes_set.emplace(id_to_join_node.at(target_id)->getId());
                        }
                    }
                }
            }

            std::vector<PlanNodeId> outer_waiting_nodes{outer_waiting_nodes_set.begin(), outer_waiting_nodes_set.end()};
            std::sort(outer_waiting_nodes.begin(), outer_waiting_nodes.end());

            for (const auto & right_id : outer_waiting_nodes)
            {
                auto right_node = id_to_join_node.at(right_id);
                if (!joined_nodes.contains(right_id))
                {
                    auto left_id = new_join_node->getId();
                    const auto & left_node = new_join_node;
                    auto left_base_table = left_node->getStep()->getType() == IQueryPlanStep::Type::TableScan;
                    auto left_stats = CardinalityEstimator::estimate(*left_node, cte_info, context);
                    if (!left_stats)
                        return {};

                    auto right_base_table = right_node->getStep()->getType() == IQueryPlanStep::Type::TableScan;
                    auto right_stats = CardinalityEstimator::estimate(*right_node, cte_info, context);
                    if (!right_stats)
                        return {};

                    // because join graph is undirected graph, only use the same edge one time.
                    Names new_left_keys;
                    Names new_right_keys;
                    for (const auto & edge : outer_edges[right_id])
                    {
                        new_left_keys.emplace_back(edge.getSourceSymbol());
                        new_right_keys.emplace_back(edge.getTargetSymbol());
                    }
                    auto join_stats = JoinEstimator::computeCardinality(
                        *left_stats.value(),
                        *right_stats.value(),
                        new_left_keys,
                        new_right_keys,
                        ASTTableJoin::Kind::Inner,
                        ASTTableJoin::Strictness::All,
                        *context,
                        left_base_table,
                        right_base_table);
                    if (!join_stats)
                        return {};

                    size_t join_card = join_stats->getRowCount();

                    selectivities.push(EdgeSelectivity{
                        left_id,
                        right_id,
                        new_left_keys[0],
                        new_right_keys[0],
                        static_cast<double>(join_card)
                            / static_cast<double>(std::max(left_stats.value()->getRowCount(), right_stats.value()->getRowCount())),
                        join_card,
                        std::min(left_stats.value()->getRowCount(), right_stats.value()->getRowCount())});
                }
            }
        }
    }

    return result;
}

PlanNodePtr SimpleReorderJoinVisitor::buildJoinTree(
    std::vector<String> & expected_output_symbols, JoinGraph & graph, PlanNodePtr join_node, ContextMutablePtr & context_ptr)
{
    PlanNodePtr result = std::move(join_node);
    const auto & filters = graph.getFilters();
    ASTPtr predicate = PredicateUtils::combineConjuncts(filters);
    if (!PredicateUtils::isTruePredicate(predicate))
    {
        auto filter_step = std::make_shared<FilterStep>(result->getStep()->getOutputStream(), predicate);
        result = std::make_shared<FilterNode>(context_ptr->nextNodeId(), std::move(filter_step), PlanNodes{result});
    }

    // If needed, introduce a projection to constrain the outputs to what was originally expected
    // Some nodes are sensitive to what's produced (e.g., DistinctLimit node)
    std::vector<String> restricted_outputs;
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & column : result->getStep()->getOutputStream().header)
    {
        if (SymbolUtils::contains(expected_output_symbols, column.name))
        {
            restricted_outputs.emplace_back(column.name);
            assignments.emplace_back(Assignment{column.name, std::make_shared<ASTIdentifier>(column.name)});
            name_to_type[column.name] = column.type;
        }
    }

    if (restricted_outputs.size() == result->getStep()->getOutputStream().header.columns())
    {
        return result;
    }

    auto restricted_outputs_step = std::make_shared<ProjectionStep>(result->getStep()->getOutputStream(), assignments, name_to_type);
    auto restricted_outputs_node
        = std::make_shared<ProjectionNode>(context_ptr->nextNodeId(), std::move(restricted_outputs_step), PlanNodes{result});

    return restricted_outputs_node;
}

}
