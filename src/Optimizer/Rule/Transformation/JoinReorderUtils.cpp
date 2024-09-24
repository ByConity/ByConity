#include <Optimizer/Rule/Transformation/JoinReorderUtils.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/MultiJoinStep.h>
#include <Optimizer/JoinGraph.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Optimizer/SymbolUtils.h>

namespace DB
{

namespace JoinReorderUtils
{
    PlanNodePtr createNewJoin(GroupId left_id, GroupId right_id, const GroupIdToIds & group_id_map, const Graph & graph, RuleContext & rule_context)
    {
        std::pair<Names, Names> join_keys = [&]() -> std::pair<Names, Names> {
            auto left_ids = group_id_map.at(left_id);
            auto right_ids = group_id_map.at(right_id);

            std::vector<std::pair<String, String>> edges;
            std::unordered_set<GroupId> right_set;
            right_set.insert(right_ids.begin(), right_ids.end());
            for (auto group_id : left_ids)
            {
                if (graph.getEdges().contains(group_id))
                {
                    for (const auto & item : graph.getEdges().at(group_id))
                    {
                        if (right_set.contains(item.first))
                        {
                            for (const auto & edge : item.second)
                            {
                                edges.emplace_back(edge.source_symbol, edge.target_symbol);
                            }
                        }
                    }
                }
            }

            const auto & union_find = graph.getUnionFind();
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
                left_sets.begin(),
                left_sets.end(),
                right_sets.begin(),
                right_sets.end(),
                std::inserter(intersect_set, intersect_set.begin()));

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
        }();

        // avoid cross join.
        if (join_keys.first.empty())
        {
            return nullptr;
        }

        const auto & memo = rule_context.optimization_context->getMemo();
        auto left = memo.getGroupById(left_id)->createLeafNode(rule_context.context);
        auto right = memo.getGroupById(right_id)->createLeafNode(rule_context.context);

        Names left_output_names = left->getOutputNames();
        NameOrderedSet left_possible_output(left_output_names.begin(), left_output_names.end());
        Names right_output_names = right->getOutputNames();
        NameOrderedSet right_possible_output(right_output_names.begin(), right_output_names.end());

        auto join_filter = PredicateUtils::combineConjuncts(JoinEnumOnGraph::getJoinFilter(graph.getFilter(), left_possible_output, right_possible_output, rule_context.context));

        NamesAndTypes output;
        for (const auto & item : left->getOutputNamesAndTypes())
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }
        for (const auto & item : right->getOutputNamesAndTypes())
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }

        auto join_step = std::make_shared<JoinStep>(
            DataStreams{left->getStep()->getOutputStream(), right->getStep()->getOutputStream()},
            DataStream{output},
            ASTTableJoin::Kind::Inner,
            ASTTableJoin::Strictness::All,
            rule_context.context->getSettingsRef().max_threads,
            rule_context.context->getSettingsRef().optimize_read_in_order,
            join_keys.first,
            join_keys.second,
            std::vector<bool>{},
            join_filter);

        return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(join_step), {left, right});
    }

    double computeFilterSelectivity(GroupId child, const Memo & memo)
    {
        if (memo.getGroupById(child)->getLogicalOtherwisePhysicalExpressions()[0]->getStep()->getType() == IQueryPlanStep::Type::Filter)
        {
            if (memo.getGroupById(child)->getLogicalOtherwisePhysicalExpressions()[0]->getChildrenGroups().size() == 1)
            {
                auto filter_child = memo.getGroupById(child)->getLogicalOtherwisePhysicalExpressions()[0]->getChildrenGroups()[0];
                if (memo.getGroupById(filter_child)->getLogicalOtherwisePhysicalExpressions()[0]->getStep()->getType() == IQueryPlanStep::Type::TableScan
                    && memo.getGroupById(child)->getStatistics().has_value()
                    && memo.getGroupById(filter_child)->getStatistics().has_value())
                {
                    return static_cast<double>(memo.getGroupById(child)->getStatistics().value()->getRowCount())
                        / memo.getGroupById(filter_child)->getStatistics().value()->getRowCount();
                }
            }
        }
        return 1.0;
    }

    void pruneJoinColumns(std::vector<String> &, PlanNodePtr &, ContextMutablePtr & )
    {
        // If needed, introduce a projection to constrain the outputs to what was originally expected
        // Some nodes are sensitive to what's produced (e.g., DistinctLimit node)
    }

}

}
