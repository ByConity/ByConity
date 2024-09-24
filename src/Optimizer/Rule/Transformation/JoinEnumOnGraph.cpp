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

#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/Task.h>
#include <Optimizer/EqualityInference.h>
#include <Optimizer/Graph.h>
#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/MultiJoinStep.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include "Optimizer/Rule/Rule.h"

namespace DB
{
ConstRefPatternPtr JoinEnumOnGraph::getPattern() const
{
    (void)support_filter;
    // return Patterns::join()
    //     .matchingStep<JoinStep>(
    //         [&](const JoinStep & s) { return s.supportReorder(support_filter) && !s.isSimpleReordered() && !s.isOrdered(); })
    //     .with(Patterns::tree(), Patterns::tree())
    //     .result();
    static auto pattern = Patterns::multiJoin()
        .result();
    return pattern;
}

static std::pair<Names, Names>
createJoinCondition(const UnionFind<String> & union_find, const std::vector<std::pair<String, String>> & edges)
{
    // extract equivalent map{representative symbol, all the symbols in the same equivalent set}
    std::unordered_map<String, std::vector<String>> left_set_to_symbols;
    std::unordered_map<String, std::vector<String>> right_set_to_symbols;


    // join key to equivalent symbols set
    for (const auto & edge : edges)
    {
        left_set_to_symbols[union_find.find(edge.first)].emplace_back(edge.first);
        right_set_to_symbols[union_find.find(edge.second)].emplace_back(edge.second);
    }

    auto extract_sorted_keys = [&](auto map) {
        std::vector<String> keys;
        boost::copy(map | boost::adaptors::map_keys, std::back_inserter(keys));
        std::sort(keys.begin(), keys.end());
        return keys;
    };
    // extract sorted representative symbol, sort the symbols because we need the rule is stable.
    auto left_sets = extract_sorted_keys(left_set_to_symbols);
    auto right_sets = extract_sorted_keys(right_set_to_symbols);

    // common equivalent symbols
    std::vector<String> intersect_set;
    std::set_intersection(
        left_sets.begin(), left_sets.end(), right_sets.begin(), right_sets.end(), std::inserter(intersect_set, intersect_set.begin()));

    // create join key using the common equivalent symbols, each equivalent set create one join criteria.
    std::vector<std::pair<String, String>> criteria;
    criteria.reserve(intersect_set.size());
    for (const auto & set : intersect_set)
    {
        criteria.emplace_back(
            *std::min_element(left_set_to_symbols[set].begin(), left_set_to_symbols[set].end()),
            *std::min_element(right_set_to_symbols[set].begin(), right_set_to_symbols[set].end()));
    }
    std::sort(criteria.begin(), criteria.end(), [](auto & a, auto & b) { return a.first < b.first; });

    Names left_join_keys;
    Names right_join_keys;
    for (const auto & item : criteria)
    {
        left_join_keys.emplace_back(item.first);
        right_join_keys.emplace_back(item.second);
    }
    return {left_join_keys, right_join_keys};
}

static PlanNodePtr createJoinNode(
    OptContextPtr & context,
    GroupId left_id,
    GroupId right_id,
    const std::pair<Names, Names> & join_keys,
    const std::set<String> & require_names,
    ASTPtr filter)
{
    if (!filter)
    {
        filter = PredicateConst::TRUE_VALUE;
    }
    auto left = context->getOptimizerContext().getMemo().getGroupById(left_id)->createLeafNode(context->getOptimizerContext().getContext());
    auto right
        = context->getOptimizerContext().getMemo().getGroupById(right_id)->createLeafNode(context->getOptimizerContext().getContext());


    NamesAndTypes output;
    NameToType name_to_type;
    for (const auto & item : left->getStep()->getOutputStream().header)
    {
        name_to_type[item.name] = item.type;
    }
    for (const auto & item : right->getStep()->getOutputStream().header)
    {
        name_to_type[item.name] = item.type;
    }
    for (const auto & name : require_names)
    {
        if (name_to_type.contains(name))
        {
            output.emplace_back(name, name_to_type[name]);
        }
    }

    auto join_step = std::make_shared<JoinStep>(
        DataStreams{left->getStep()->getOutputStream(), right->getStep()->getOutputStream()},
        DataStream{output},
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        context->getOptimizerContext().getContext()->getSettingsRef().max_threads,
        context->getOptimizerContext().getContext()->getSettingsRef().optimize_read_in_order,
        join_keys.first,
        join_keys.second,
        std::vector<bool>{},
        filter,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);

    return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(join_step), {left, right});
}

static std::set<String> createPossibleSymbols(const std::vector<GroupId> & groups, OptContextPtr & context)
{
    std::set<String> result;
    for (auto id : groups)
    {
        for (const auto & col : context->getMemo().getGroupById(id)->getStep()->getOutputStream().header)
        {
            result.insert(col.name);
        }
    }
    return result;
}


ConstASTs JoinEnumOnGraph::getJoinFilter(
    const ASTPtr & all_filter, std::set<String> & left_symbols, std::set<String> & right_symbols, ContextMutablePtr & context)
{
    auto all_filter_inference = EqualityInference::newInstance(all_filter, context);
    auto non_inferrable_conjuncts = EqualityInference::nonInferrableConjuncts(all_filter, context);

    std::set<String> union_scope;
    union_scope.insert(left_symbols.begin(), left_symbols.end());
    union_scope.insert(right_symbols.begin(), right_symbols.end());

    std::vector<ConstASTPtr> join_predicates_builder;
    for (auto & tmp_conjunct : non_inferrable_conjuncts)
    {
        auto conjunct = all_filter_inference.rewrite(tmp_conjunct, union_scope);
        if (conjunct && all_filter_inference.rewrite(tmp_conjunct, left_symbols) == nullptr
            && all_filter_inference.rewrite(tmp_conjunct, right_symbols) == nullptr)
        {
            join_predicates_builder.emplace_back(conjunct);
        }
    }

    auto join_equalities = all_filter_inference.partitionedBy(union_scope).getScopeEqualities();
    EqualityInference join_inference = EqualityInference::newInstance(join_equalities, context);
    auto straddling = join_inference.partitionedBy(left_symbols).getScopeStraddlingEqualities();
    join_predicates_builder.insert(join_predicates_builder.end(), straddling.begin(), straddling.end());

    return join_predicates_builder;
}


static GroupId buildJoinNode(
    OptContextPtr & context,
    std::vector<GroupId> groups,
    const UnionFind<String> & union_find,
    const Graph & graph,
    const std::set<String> & require_names,
    const ASTPtr & all_filter)
{
    // if only one group, return leaf node.
    if (groups.size() == 1)
    {
        return groups[0];
    }

    auto left_groups = std::vector<GroupId>(groups.begin(), groups.begin() + 1);
    auto right_groups = std::vector<GroupId>(groups.begin() + 1, groups.end());

    auto join_keys = createJoinCondition(union_find, graph.bridges(left_groups, right_groups));
    auto left_possible_output = createPossibleSymbols(left_groups, context);
    auto right_possible_output = createPossibleSymbols(right_groups, context);
    // create join filter
    ContextMutablePtr ptr = context->getOptimizerContext().getContext();
    auto filter
        = PredicateUtils::combineConjuncts(JoinEnumOnGraph::getJoinFilter(all_filter, left_possible_output, right_possible_output, ptr));
    auto filter_symbols = SymbolsExtractor::extract(filter);

    // build left node, using first group
    std::set<String> require_left = require_names;
    require_left.insert(join_keys.first.begin(), join_keys.first.end());
    for (const auto & symbol : filter_symbols)
    {
        if (left_possible_output.contains(symbol))
        {
            require_left.insert(symbol);
        }
    }
    auto left_id = buildJoinNode(context, left_groups, union_find, graph, require_left, all_filter);

    // build right node, using [1:] groups
    std::set<String> require_right = require_names;
    require_right.insert(join_keys.second.begin(), join_keys.second.end());
    for (const auto & symbol : filter_symbols)
    {
        if (right_possible_output.contains(symbol))
        {
            require_right.insert(symbol);
        }
    }
    auto right_id = buildJoinNode(context, right_groups, union_find, graph, require_right, all_filter);

    if (left_id > right_id)
    {
        join_keys = {join_keys.second, join_keys.first};
        std::swap(left_id, right_id);
    }

    auto join_node = createJoinNode(context, left_id, right_id, join_keys, require_names, filter);
    GroupExprPtr join_expr;
    context->getOptimizerContext().recordPlanNodeIntoGroup(join_node, join_expr, RuleType::JOIN_ENUM_ON_GRAPH);
    join_expr->setRuleExplored(RuleType::INNER_JOIN_COMMUTATION);

    join_node = createJoinNode(context, right_id, left_id, {join_keys.second, join_keys.first}, require_names, filter);
    context->getOptimizerContext().recordPlanNodeIntoGroup(join_node, join_expr, RuleType::JOIN_ENUM_ON_GRAPH, join_expr->getGroupId());
    join_expr->setRuleExplored(RuleType::INNER_JOIN_COMMUTATION);

    return join_expr->getGroupId();
}

TransformResult JoinEnumOnGraph::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto group_id = context.group_id;
    auto group = context.optimization_context->getOptimizerContext().getMemo().getGroupById(group_id);

    const auto * join_step = dynamic_cast<const MultiJoinStep *>(node->getStep().get());

    if (join_step->getGraph().getNodes().size() > context.context->getSettingsRef().max_graph_reorder_size)
        return {};

    std::set<String> output_names;
    for (const auto & item : group->getStep()->getOutputStream().header)
    {
        output_names.insert(item.name);
    }

    PlanNodes result;
    std::bitset<Memo::MAX_JOIN_ROOT_ID> intersection;
    const auto & source_to_root = context.optimization_context->getMemo().getSourceToJoinRoot();
    intersection.flip();
    bool found_root = true;
    for (auto left_group_id : join_step->getGraph().getNodes())
    {
        if (!source_to_root.contains(left_group_id))
        {
            found_root = false;
            break;
        }
        intersection &= source_to_root.at(left_group_id);
    }

    if (intersection.count() && found_root)
    {
        return {};
    }

    const auto & graph = join_step->getGraph();
    for (auto & partition : graph.cutPartitions())
    {
        auto left_groups = graph.getDFSOrder(partition.left);
        std::reverse(left_groups.begin(), left_groups.end());
        auto right_groups = graph.getDFSOrder(partition.right);
        std::reverse(right_groups.begin(), right_groups.end());

        // create join keys
        auto join_keys = createJoinCondition(graph.getUnionFind(), graph.bridges(left_groups, right_groups));

        auto left_possible_output = createPossibleSymbols(left_groups, context.optimization_context);
        auto right_possible_output = createPossibleSymbols(right_groups, context.optimization_context);
        // create join filter
        auto filter = PredicateUtils::combineConjuncts(
            getJoinFilter(graph.getFilter(), left_possible_output, right_possible_output, context.context));
        auto filter_symbols = SymbolsExtractor::extract(filter);

        // build left node
        std::set<String> require_left = output_names;
        require_left.insert(join_keys.first.begin(), join_keys.first.end());
        for (const auto & symbol : filter_symbols)
        {
            if (left_possible_output.contains(symbol))
            {
                require_left.insert(symbol);
            }
        }
        auto new_left_id
            = buildJoinNode(context.optimization_context, left_groups, graph.getUnionFind(), graph, require_left, graph.getFilter());

        // build right node
        std::set<String> require_right = output_names;
        require_right.insert(join_keys.second.begin(), join_keys.second.end());
        for (const auto & symbol : filter_symbols)
        {
            if (right_possible_output.contains(symbol))
            {
                require_right.insert(symbol);
            }
        }
        auto new_right_id
            = buildJoinNode(context.optimization_context, right_groups, graph.getUnionFind(), graph, require_right, graph.getFilter());


        result.emplace_back(createJoinNode(context.optimization_context, new_left_id, new_right_id, join_keys, output_names, filter));
        result.emplace_back(createJoinNode(
            context.optimization_context,
            new_right_id,
            new_left_id,
            std::make_pair(join_keys.second, join_keys.first),
            output_names,
            filter));
    }

    return TransformResult{result};
}

const std::vector<RuleType> & JoinEnumOnGraph::blockRules() const
{
    static std::vector<RuleType> block{RuleType::JOIN_ENUM_ON_GRAPH, RuleType::INNER_JOIN_COMMUTATION};
    return block;
}

// Graph Graph::fromJoinSet(RuleContext & context, JoinSet & join_set)
// {
//     Graph graph;

//     std::unordered_map<String, GroupId> symbol_to_group_id;
//     for (auto group_id : join_set.getGroups())
//     {
//         for (const auto & symbol : context.optimization_context->getMemo().getGroupById(group_id)->getStep()->getOutputStream().header)
//         {
//             assert(!symbol_to_group_id.contains(symbol.name)); // duplicate symbol
//             symbol_to_group_id[symbol.name] = group_id;
//         }
//         graph.nodes.emplace_back(group_id);
//     }

//     for (auto & sets : join_set.getUnionFind().getSets())
//     {
//         for (const auto & source_symbol : sets)
//         {
//             for (const auto & target_symbol : sets)
//             {
//                 Utils::checkState(symbol_to_group_id.contains(source_symbol));
//                 Utils::checkState(symbol_to_group_id.contains(target_symbol));
//                 auto source_id = symbol_to_group_id.at(source_symbol);
//                 auto target_id = symbol_to_group_id.at(target_symbol);
//                 if (source_id != target_id)
//                 {
//                     graph.edges[source_id][target_id].emplace_back(source_symbol, target_symbol);
//                 }
//             }
//         }
//     }

//     return graph;
// }

}
