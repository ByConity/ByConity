#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/CrossJoinToUnion.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/UnionStep.h>
#include <Common/Exception.h>
#include <common/types.h>

namespace DB
{

ConstRefPatternPtr CrossJoinToUnion::getPattern() const
{
    static auto pattern = Patterns::join().matchingStep<JoinStep>([&](const JoinStep & s) { return s.isCrossJoin() && matchPattern(s); }).result();
    return pattern;
}

TransformResult CrossJoinToUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;

    auto step_ptr = node->getStep();
    const auto & step = dynamic_cast<const JoinStep &>(*step_ptr);
    const auto & filter = step.getFilter();

    ConstASTPtr match_filter = splitFilter(filter).first;
    ConstASTPtr un_match_filter = splitFilter(filter).second;

    std::vector<ConstASTPtr> disjuncts = PredicateUtils::extractDisjuncts(match_filter);

    std::vector<PlanNodeAndMappings> copy_join_nodes;
    for (size_t i = 0; i < disjuncts.size(); i++)
    {
        auto child_mapping = PlanSymbolReallocator::reallocate(node, context);
        copy_join_nodes.emplace_back(child_mapping);
    }

    DataStream output_stream = step.getOutputStream();
    DataStreams input_streams;
    OutputToInputs output_to_inputs;
    PlanNodes children;

    for (size_t i = 0; i < copy_join_nodes.size(); i++)
    {
        auto & child_mapping = copy_join_nodes.at(i);
        input_streams.emplace_back(child_mapping.plan_node->getStep()->getOutputStream());

        for (auto & entry : child_mapping.mappings)
        {
            auto & outputs = output_to_inputs[entry.first];
            outputs.emplace_back(entry.second);
        }

        auto child = child_mapping.plan_node;
        auto child_step_ptr = child->getStep();
        const auto & child_step = dynamic_cast<const JoinStep &>(*child_step_ptr);
        const auto & child_filter = child_step.getFilter();
        ConstASTPtr child_match_filter = splitFilter(child_filter).first;
        ConstASTPtr child_un_match_filter = splitFilter(child_filter).second;

        std::vector<ConstASTPtr> child_disjuncts = PredicateUtils::extractDisjuncts(child_match_filter);

        DataStream output;
        for (const auto & input : child_step.getInputStreams())
        {
            for (const auto & column : input.header)
            {
                output.header.insert(column);
            }
        }

        if (i == 0)
        {
            auto join_step = std::make_shared<JoinStep>(
                child_step.getInputStreams(),
                output,
                child_step.getKind(),
                child_step.getStrictness(),
                context->getSettingsRef().max_threads,
                context->getSettingsRef().optimize_read_in_order,
                child_step.getLeftKeys(),
                child_step.getRightKeys(),
                child_step.getKeyIdsNullSafe(),
                PredicateConst::TRUE_VALUE,
                child_step.isHasUsing(),
                child_step.getRequireRightKeys(),
                child_step.getAsofInequality(),
                child_step.getDistributionType());
            PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), child->getChildren());

            std::vector<ConstASTPtr> predicates = {child_un_match_filter, child_disjuncts.at(i)};
            auto filter_step
                = std::make_shared<FilterStep>(join_node->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(predicates));
            PlanNodePtr filter_node = std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{join_node});

            children.emplace_back(filter_node);
        }
        else
        {
            std::vector<ConstASTPtr> predicates;
            for (size_t j = 0; j < child_disjuncts.size(); j++)
            {
                if (j != i)
                {
                    ASTPtr ptr = child_disjuncts.at(j)->clone();
                    predicates.emplace_back(makeASTFunction("not", ASTs{ptr}));
                }
                else
                {
                    predicates.emplace_back(child_disjuncts.at(j)->clone());
                }
            }
            predicates.emplace_back(child_un_match_filter);

            auto join_step = std::make_shared<JoinStep>(
                child_step.getInputStreams(),
                output,
                child_step.getKind(),
                child_step.getStrictness(),
                context->getSettingsRef().max_threads,
                context->getSettingsRef().optimize_read_in_order,
                child_step.getLeftKeys(),
                child_step.getRightKeys(),
                child_step.getKeyIdsNullSafe(),
                PredicateConst::TRUE_VALUE,
                child_step.isHasUsing(),
                child_step.getRequireRightKeys(),
                child_step.getAsofInequality(),
                child_step.getDistributionType());
            PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), child->getChildren());

            auto filter_step
                = std::make_shared<FilterStep>(join_node->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(predicates));
            PlanNodePtr filter_node = std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{join_node});

            children.emplace_back(filter_node);
        }
    }

    auto union_step = std::make_shared<UnionStep>(input_streams, output_stream, output_to_inputs);
    auto union_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(union_step), children);
    return union_node;
}

bool CrossJoinToUnion::matchPattern(const JoinStep & step)
{
    const auto & filter = step.getFilter();
    if (PredicateUtils::isTruePredicate(filter))
    {
        return false;
    }

    /// if predicate match :  (A = B) OR (C = D) OR (E = F), return true
    const auto & fun = filter->template as<const ASTFunction &>();
    if (fun.name == "or")
    {
        return hasDNFFilters(filter);
    }
    /// if predicate match :  (A = B) OR (C = D) OR (E = F) AND D, return true.
    if (fun.name == "and")
    {
        auto conjuncts = PredicateUtils::extractConjuncts(filter);

        /// if predicate match :  (A = B) OR (C = D) OR (E = F) AND D AND E, return false
        if (conjuncts.size() != 2)
            return false;

        for (auto & conjunct : conjuncts)
        {
            if (hasDNFFilters(conjunct))
                return true;
        }
        return false;
    }
    return false;
}

bool CrossJoinToUnion::hasDNFFilters(const ConstASTPtr & filter)
{
    /// predicate format : (A = B)
    auto matched_disjunct = [&](const auto & disjunct) -> bool {
        if (disjunct && disjunct->template as<const ASTFunction>())
        {
            const auto & fun = disjunct->template as<const ASTFunction &>();
            if (fun.name == "equals" && fun.arguments->children.size() == 2
                && fun.arguments->children[0]->getType() == ASTType::ASTIdentifier
                && fun.arguments->children[1]->getType() == ASTType::ASTIdentifier)
            {
                return true;
            }
        }
        return false;
    };

    auto disjuncts = PredicateUtils::extractDisjuncts(filter);

    /// disjuncts at least have 2 predicates, combine with OR function.
    if (disjuncts.size() < 2)
    {
        return false;
    }

    /// all disjuncts must satisfied with format (A = B), A, B both are identifiers.
    bool matched_disjuncts = true;
    for (auto & disjunct : disjuncts)
    {
        if (!matched_disjunct(disjunct))
        {
            matched_disjuncts = false;
        }
    }

    return matched_disjuncts;
}

std::pair<ConstASTPtr, ConstASTPtr> CrossJoinToUnion::splitFilter(const ConstASTPtr & filter)
{
    const auto & fun = filter->template as<const ASTFunction &>();
    /// if predicate match :  (A = B) OR (C = D) OR (E = F),
    /// return <(A = B) OR (C = D) OR (E = F), true>
    if (fun.name == "or")
    {
        return std::make_pair(filter, PredicateConst::TRUE_VALUE);
    }
    /// if predicate match :  (A = B) OR (C = D) OR (E = F) AND D
    /// return <(A = B) OR (C = D) OR (E = F), D>
    if (fun.name == "and")
    {
        auto conjuncts = PredicateUtils::extractConjuncts(filter);

        Utils::checkArgument(conjuncts.size() == 2, "conjuncts size must be 2.");
        if (hasDNFFilters(conjuncts.at(0)))
            return std::make_pair(conjuncts[0], conjuncts[1]);
        if (hasDNFFilters(conjuncts.at(1)))
            return std::make_pair(conjuncts[1], conjuncts[0]);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalidate filter predicate, {}", filter->formatForErrorMessage());
}

}
