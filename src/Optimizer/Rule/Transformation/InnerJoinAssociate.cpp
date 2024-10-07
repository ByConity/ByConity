#include <Optimizer/Rule/Transformation/InnerJoinAssociate.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{

const std::vector<RuleType> & InnerJoinAssociate::blockRules() const
{
    // static std::vector<RuleType> rules{RuleType::INNER_JOIN_COMMUTATION, RuleType::INNER_JOIN_ASSOCIATE};
    static std::vector<RuleType> rules{};
    return rules;
}

ConstRefPatternPtr InnerJoinAssociate::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) {
            return supportSwap(s) && !s.isOrdered() && s.getFilter() && !PredicateUtils::isTruePredicate(s.getFilter());
        })
        .with(
            Patterns::join()
                .matchingStep<JoinStep>([&](const JoinStep & s) { return supportSwap(s) && !s.isOrdered(); })
                .with(Patterns::tree(), Patterns::tree()),
            Patterns::tree())
        .result();
    return pattern;
}

/*
 *      Join            Join
 *      /    \          /    \
 *     Join   C  =>    A     Join
 *    /    \                /    \
 *   A      B              B      C
 */
TransformResult InnerJoinAssociate::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!rule_context.optimization_context->getMemo().getGroupById(rule_context.group_id)->getCTESet().empty())
    {
        return {};
    }
    auto * join_node = dynamic_cast<JoinNode *>(node.get());
    if (!join_node)
        return {};

    auto join_step = join_node->getStep();

    auto * left_join_node = dynamic_cast<JoinNode *>(node->getChildren()[0].get());
    auto left_join_step = left_join_node->getStep();

    // TODO
    if (join_step->hasKeyIdNullSafe() || left_join_step->hasKeyIdNullSafe())
        return {};

    auto a = left_join_node->getChildren()[0];
    auto b = left_join_node->getChildren()[1];
    auto c = node->getChildren()[1];

    // LOG_DEBUG(
    //     getLogger("InnerJoinAssociate"),
    //     fmt::format(
    //         "hint {}/{}/{}",
    //         dynamic_cast<AnyStep &>(*a->getStep()).getGroupId(),
    //         dynamic_cast<AnyStep &>(*b->getStep()).getGroupId(),
    //         dynamic_cast<AnyStep &>(*c->getStep()).getGroupId()));

    ASTs conjuncts;
    conjuncts.emplace_back(PredicateUtils::extractJoinPredicate(*join_node));
    conjuncts.emplace_back(PredicateUtils::extractJoinPredicate(*left_join_node));
    ASTPtr all_filter = PredicateUtils::combineConjuncts(conjuncts);


    auto a_names = a->getCurrentDataStream().header.getNames();
    auto b_names = b->getCurrentDataStream().header.getNames();
    auto c_names = c->getCurrentDataStream().header.getNames();
    std::set<String> a_name_set{a_names.begin(), a_names.end()};
    std::set<String> b_name_set{b_names.begin(), b_names.end()};
    std::set<String> c_name_set{c_names.begin(), c_names.end()};

    std::set<String> bc_name_set{b_names.begin(), b_names.end()};
    bc_name_set.insert(c_names.begin(), c_names.end());


    auto sort_criteria = [](Names & left_keys, Names & right_keys) {
        std::vector<std::pair<String, String>> criteria;

        for (size_t i = 0; i < left_keys.size(); i++)
        {
            criteria.emplace_back(left_keys[i], right_keys[i]);
        }
        std::sort(criteria.begin(), criteria.end(), [](auto & lho, auto & rho) { return lho.first < rho.first; });
        left_keys.clear();
        right_keys.clear();
        for (auto & item : criteria)
        {
            left_keys.emplace_back(item.first);
            right_keys.emplace_back(item.second);
        }
    };

    /*
    * a join bc
    */
    Names top_left_keys;
    Names top_right_keys;
    ASTPtr top_join_filter;
    {
        auto top_join_predicates = JoinEnumOnGraph::getJoinFilter(all_filter, a_name_set, bc_name_set, rule_context.context);
        auto top_predicates = PredicateUtils::extractEqualPredicates(top_join_predicates);

        // join clauses for [a join bc]
        for (const auto & clause : top_predicates.first)
        {
            auto left_key = clause.first->as<ASTIdentifier>()->name();
            auto right_key = clause.second->as<ASTIdentifier>()->name();
            if (a_name_set.contains(left_key))
            {
                top_left_keys.emplace_back(left_key);
                top_right_keys.emplace_back(right_key);
            }
            else
            {
                top_left_keys.emplace_back(right_key);
                top_right_keys.emplace_back(left_key);
            }
        }
        sort_criteria(top_left_keys, top_right_keys);
        // join filter for [a join bc]
        top_join_filter = PredicateUtils::combineConjuncts(top_predicates.second);

        if (ASTEquality::ASTEquals()(top_join_filter, join_step->getFilter()))
            return {};
    }

    /*
    * b join c
    */
    Names bc_left_keys;
    Names bc_right_keys;
    ASTPtr bc_join_filter;
    {
        auto bc_join_predicates = JoinEnumOnGraph::getJoinFilter(all_filter, b_name_set, c_name_set, rule_context.context);
        auto bc_predicates = PredicateUtils::extractEqualPredicates(bc_join_predicates);

        // join clauses for [a join bc]
        for (const auto & clause : bc_predicates.first)
        {
            auto left_key = clause.first->as<ASTIdentifier>()->name();
            auto right_key = clause.second->as<ASTIdentifier>()->name();
            if (b_name_set.contains(left_key))
            {
                bc_left_keys.emplace_back(left_key);
                bc_right_keys.emplace_back(right_key);
            }
            else
            {
                bc_left_keys.emplace_back(right_key);
                bc_right_keys.emplace_back(left_key);
            }
        }
        sort_criteria(bc_left_keys, bc_right_keys);
        // join filter for [a join bc]
        bc_join_filter = PredicateUtils::combineConjuncts(bc_predicates.second);
    }


    // bc join step
    NamesAndTypes bc_output;
    {
        std::set<String> require_names{top_right_keys.begin(), top_right_keys.end()};
        auto filter_symbols = SymbolsExtractor::extract(top_join_filter);
        require_names.insert(filter_symbols.begin(), filter_symbols.end());
        auto output_names = node->getOutputNames();
        require_names.insert(output_names.begin(), output_names.end());

        NameToType name_to_type;
        for (const auto & item : b->getStep()->getOutputStream().header)
            if (require_names.contains(item.name))
            {
                bc_output.emplace_back(item.name, item.type);
            }
        for (const auto & item : c->getStep()->getOutputStream().header)
            if (require_names.contains(item.name))
            {
                bc_output.emplace_back(item.name, item.type);
            }
    }

    auto bc_join_step = std::make_shared<JoinStep>(
        DataStreams{b->getStep()->getOutputStream(), c->getStep()->getOutputStream()},
        DataStream{bc_output},
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        rule_context.context->getSettingsRef().max_threads,
        rule_context.context->getSettingsRef().optimize_read_in_order,
        bc_left_keys,
        bc_right_keys,
        std::vector<bool>{},
        bc_join_filter);
    auto bc_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), bc_join_step, {b, c});


    auto top_join_step = std::make_shared<JoinStep>(
        DataStreams{a->getStep()->getOutputStream(), bc_join_step->getOutputStream()},
        node->getCurrentDataStream(),
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        rule_context.context->getSettingsRef().max_threads,
        rule_context.context->getSettingsRef().optimize_read_in_order,
        top_left_keys,
        top_right_keys,
        std::vector<bool>{},
        top_join_filter);
    auto top_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), top_join_step, {a, bc_node});

    return top_node;
}

}
