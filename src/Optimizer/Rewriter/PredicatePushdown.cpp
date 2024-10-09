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

#include <memory>
#include <Optimizer/Rewriter/PredicatePushdown.h>

#include <Analyzers/TypeAnalyzer.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/join_common.h>
#include <Optimizer/DomainTranslator.h>
#include <Optimizer/EqualityInference.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/ExpressionEquivalence.h>
#include <Optimizer/ExpressionInliner.h>
#include <Optimizer/ExpressionInterpreter.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/ProjectionPlanner.h>
#include <Optimizer/Rule/Rewrite/SimplifyExpressionRules.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Optimizer/SymbolUtils.h>
#include <Optimizer/Utils.h>
#include <Optimizer/makeCastFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/ArrayJoinStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/UnionStep.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int PLAN_BUILD_ERROR;
}

bool PredicatePushdown::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    auto cte_reference_counts = plan.getCTEInfo().collectCTEReferenceCounts(plan.getPlanNode());
    PredicateVisitor visitor{pushdown_filter_into_cte, simplify_common_filter, context, plan.getCTEInfo(), cte_reference_counts};
    PredicateContext predicate_context{
        .predicate = PredicateConst::TRUE_VALUE, .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE, .context = context};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, predicate_context);
    plan.update(result);
    return true;
}

PlanNodePtr PredicateVisitor::visitPlanNode(PlanNodeBase & node, PredicateContext & predicate_context)
{
    PredicateContext true_context{
        .predicate = PredicateConst::TRUE_VALUE,
        .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE,
        .context = predicate_context.context};
    PlanNodePtr rewritten = processChild(node, true_context);
    if (!PredicateUtils::isTruePredicate(predicate_context.predicate))
    {
        // we cannot push our predicate down any further
        auto name_and_types = rewritten->getStep()->getOutputStream().getNamesToTypes();
        auto remaining_filter = ExpressionInterpreter::optimizePredicate(predicate_context.predicate, name_and_types, context);
        Predicate::DomainTranslator<String> domain_translator{context};
        auto extract_result = domain_translator.getExtractionResult(remaining_filter, name_and_types);

        if (!domain_translator.isIgnored() && remaining_filter->getColumnName() != extract_result.remaining_expression->getColumnName())
        {
            ASTPtr combine_extraction_result = PredicateUtils::combineConjuncts(
                ASTs{domain_translator.toPredicate(extract_result.tuple_domain), extract_result.remaining_expression});

            // the processed predicate is simpler than the original predicate
            if (combine_extraction_result->getColumnName().size() < remaining_filter->getColumnName().size())
                remaining_filter = combine_extraction_result;
        }

        auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), remaining_filter);
        auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
        return filter_node;
    }
    return rewritten;
}

PlanNodePtr PredicateVisitor::visitProjectionNode(ProjectionNode & node, PredicateContext & predicate_context)
{
    const auto & step = *node.getStep();
    const auto & assignments = step.getAssignments();
    std::set<String> deterministic_symbols = ExpressionDeterminism::getDeterministicSymbols(assignments, context);

    // Push down conjuncts from the inherited predicate that only
    // depend on deterministic assignments with certain limitations.
    std::vector<ConstASTPtr> deterministic_conjuncts;
    std::vector<ConstASTPtr> non_deterministic_conjuncts;
    std::vector<ConstASTPtr> predicates = PredicateUtils::extractConjuncts(predicate_context.predicate);
    for (auto & predicate : predicates)
    {
        std::set<std::string> symbols = SymbolsExtractor::extract(predicate);
        bool contains = true;
        for (const auto & symbol : symbols)
        {
            if (!deterministic_symbols.contains(symbol))
            {
                contains = false;
            }
        }
        if (contains)
        {
            deterministic_conjuncts.emplace_back(predicate);
        }
        else
        {
            non_deterministic_conjuncts.emplace_back(predicate);
        }
    }

    // We partition the expressions in the deterministic_conjuncts into two lists,
    // and only inline the expressions that are in the inlining targets list.
    std::vector<ConstASTPtr> inlining_conjuncts;
    std::vector<ConstASTPtr> non_inlining_conjuncts;
    for (auto & conjunct : deterministic_conjuncts)
    {
        if (PredicateUtils::isInliningCandidate(conjunct, node))
        {
            inlining_conjuncts.emplace_back(conjunct);
        }
        else
        {
            non_inlining_conjuncts.emplace_back(conjunct);
        }
    }


    std::vector<ConstASTPtr> inlined_deterministic_conjuncts;
    for (auto & conjunct : inlining_conjuncts)
    {
        auto inlined = ExpressionInliner::inlineSymbols(conjunct, assignments);
        inlined_deterministic_conjuncts.emplace_back(inlined);
    }

    auto pushdown_predicate = PredicateUtils::combineConjuncts(inlined_deterministic_conjuncts);
    LOG_DEBUG(
        getLogger("PredicateVisitor"),
        "project node {}, pushdown_predicate : {}",
        node.getId(),
        pushdown_predicate->formatForErrorMessage());

    if (!pushdown_predicate->as<ASTLiteral>())
        pushdown_predicate
            = ExpressionInterpreter::optimizePredicate(pushdown_predicate, step.getInputStreams()[0].getNamesToTypes(), context);
    PredicateContext expression_context{
        .predicate = pushdown_predicate,
        .extra_predicate_for_simplify_outer_join
        = ExpressionInliner::inlineSymbols(predicate_context.extra_predicate_for_simplify_outer_join, assignments),
        .context = predicate_context.context};
    PlanNodePtr rewritten = processChild(node, expression_context);

    // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
    // if any, will be in the filter node.
    for (auto & conjunct : non_deterministic_conjuncts)
    {
        non_inlining_conjuncts.emplace_back(conjunct);
    }
    if (!non_inlining_conjuncts.empty())
    {
        auto filter_step = std::make_shared<FilterStep>(
            rewritten->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(non_inlining_conjuncts));
        auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
        rewritten = filter_node;
    }
    return rewritten;
}

PlanNodePtr PredicateVisitor::visitFilterNode(FilterNode & node, PredicateContext & predicate_context)
{
    const auto & step = *node.getStep();

    // handle in function has large value list
    UInt64 limit = predicate_context.context->getSettingsRef().max_in_value_list_to_pushdown;
    std::pair<ConstASTPtr, ConstASTPtr> split_in_filter = FilterStep::splitLargeInValueList(step.getFilter(), limit);

    LOG_DEBUG(
        getLogger("PredicateVisitor"),
        "filter node {}, split_in_filter.first : {}, split_in_filter.second : {}",
        node.getId(),
        split_in_filter.first->formatForErrorMessage(),
        split_in_filter.second->formatForErrorMessage());

    auto predicates = std::vector<ConstASTPtr>{split_in_filter.first, predicate_context.predicate};
    ConstASTPtr predicate = PredicateUtils::combineConjuncts(predicates);

    if (simplify_common_filter)
    {
        predicate = CommonPredicatesRewriter::rewrite(predicate, context);
    }

    LOG_DEBUG(
        getLogger("PredicateVisitor"),
        "filter node {}, pushdown_predicate : {}",
        node.getId(),
        predicate->formatForErrorMessage());

    PredicateContext filter_context{
        .predicate = predicate,
        .extra_predicate_for_simplify_outer_join = predicate_context.extra_predicate_for_simplify_outer_join,
        .context = predicate_context.context};
    PlanNodePtr rewritten = process(*node.getChildren()[0], filter_context);

    if (rewritten->getStep()->getType() != IQueryPlanStep::Type::Filter)
    {
        if (!PredicateUtils::isTruePredicate(split_in_filter.second))
        {
            auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), split_in_filter.second);
            return std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
        }
        return rewritten;
    }

    if (rewritten->getStep()->getType() == IQueryPlanStep::Type::Filter)
    {
        if (rewritten->getChildren()[0] != node.getChildren()[0])
        {
            if (!PredicateUtils::isTruePredicate(split_in_filter.second))
            {
                auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), split_in_filter.second);
                return std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
            }
            return rewritten;
        }
        auto rewritten_step_ptr = rewritten->getStep();
        const auto & rewritten_step = dynamic_cast<const FilterStep &>(*rewritten_step_ptr);

        // TODO a more reasonable way to do this
        // see ExpressionEquivalence
        if (step.getFilter() != rewritten_step.getFilter())
        {
            if (!PredicateUtils::isTruePredicate(split_in_filter.second))
            {
                auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), split_in_filter.second);
                return std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
            }
            return rewritten;
        }
    }
    return node.shared_from_this();
}

PlanNodePtr PredicateVisitor::visitAggregatingNode(AggregatingNode & node, PredicateContext & predicate_context)
{
    const auto & step = *node.getStep();
    const auto & keys = step.getKeys();

    if (keys.empty())
    {
        return visitPlanNode(node, predicate_context);
    }

    // never push predicate through grouping sets agg
    if (step.isGroupingSet())
    {
        return visitPlanNode(node, predicate_context);
    }

    ConstASTPtr inherited_predicate = predicate_context.predicate;
    EqualityInference equality_inference = EqualityInference::newInstance(inherited_predicate, context);

    std::vector<ConstASTPtr> pushdown_conjuncts;
    std::vector<ConstASTPtr> post_aggregation_conjuncts;

    // Strip out non-deterministic conjuncts
    for (auto & conjunct : PredicateUtils::extractConjuncts(inherited_predicate))
    {
        if (!ExpressionDeterminism::isDeterministic(conjunct, context))
        {
            post_aggregation_conjuncts.emplace_back(conjunct);
        }
    }

    inherited_predicate = ExpressionDeterminism::filterDeterministicConjuncts(inherited_predicate, context);

    // Sort non-equality predicates by those that can be pushed down and those that cannot
    std::set<String> grouping_keys;
    for (const auto & key : keys)
    {
        grouping_keys.emplace(key);
    }

    for (auto & conjunct : EqualityInference::nonInferrableConjuncts(inherited_predicate, context))
    {
        ASTPtr rewritten = equality_inference.rewrite(conjunct, grouping_keys);
        if (rewritten != nullptr)
        {
            pushdown_conjuncts.emplace_back(rewritten);
        }
        else
        {
            post_aggregation_conjuncts.emplace_back(conjunct);
        }
    }

    // Add the equality predicates back in
    EqualityPartition equality_partition = equality_inference.partitionedBy(grouping_keys);
    for (const auto & conjunct : equality_partition.getScopeEqualities())
    {
        pushdown_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : equality_partition.getScopeComplementEqualities())
    {
        post_aggregation_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : equality_partition.getScopeStraddlingEqualities())
    {
        post_aggregation_conjuncts.emplace_back(conjunct);
    }

    PredicateContext agg_context{
        .predicate = PredicateUtils::combineConjuncts(pushdown_conjuncts),
        .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE,
        .context = predicate_context.context};
    PlanNodePtr rewritten = process(*node.getChildren()[0], agg_context);

    PlanNodePtr output = node.shared_from_this();
    if (rewritten != node.getChildren()[0])
    {
        output = PlanNodeBase::createPlanNode(context->nextNodeId(), node.getStep(), PlanNodes{rewritten}, node.getStatistics());
    }
    if (!post_aggregation_conjuncts.empty())
    {
        auto filter_step = std::make_shared<FilterStep>(
            output->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(post_aggregation_conjuncts));
        output = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{output});
    }
    return output;
}

PlanNodePtr PredicateVisitor::visitJoinNode(JoinNode & node, PredicateContext & predicate_context)
{
    ConstASTPtr inherited_predicate = CommonPredicatesRewriter::rewrite(predicate_context.predicate, context, true);
    auto step = node.getStep();

    // RequireRightKeys is clickhouse sql only, we don't process this kind of join.
    if (step->getRequireRightKeys().has_value() || step->hasKeyIdNullSafe())
    {
        return visitPlanNode(node, predicate_context);
    }

    // Asof is clickhouse sql only, we don't process this kind of join.
    if (step->getStrictness() == ASTTableJoin::Strictness::Asof)
    {
        return visitPlanNode(node, predicate_context);
    }

    PlanNodePtr & left = node.getChildren()[0];
    PlanNodePtr & right = node.getChildren()[1];
    ConstASTPtr left_effective_predicate = EffectivePredicateExtractor::extract(left, context);
    ConstASTPtr right_effective_predicate = EffectivePredicateExtractor::extract(right, context);

    ConstASTPtr join_predicate = PredicateUtils::extractJoinPredicate(node);

    std::set<String> left_symbols;
    for (const auto & column : left->getStep()->getOutputStream().header)
    {
        left_symbols.emplace(column.name);
    }
    std::set<String> right_symbols;
    for (const auto & column : right->getStep()->getOutputStream().header)
    {
        right_symbols.emplace(column.name);
    }

    std::vector<ConstASTPtr> conjuncts_for_simplify_outer_join{
        inherited_predicate, predicate_context.extra_predicate_for_simplify_outer_join};
    auto predicate_for_simplify_outer_join = PredicateUtils::combineConjuncts(conjuncts_for_simplify_outer_join);
    tryNormalizeOuterToInnerJoin(node, predicate_for_simplify_outer_join, context);
    step = node.getStep(); // update step since it may be changed by `tryNormalizeOuterToInnerJoin`

    ConstASTPtr left_predicate;
    ConstASTPtr right_predicate;
    ConstASTPtr post_join_predicate;
    ConstASTPtr new_join_predicate;

    ASTTableJoin::Kind kind = step->getKind();

    LOG_DEBUG(
        getLogger("PredicateVisitor"),
        "join node {}, inherited_predicate : {}, left effective predicate: {} , right effective predicate: {}, join_predicate : {}",
        node.getId(),
        inherited_predicate->formatForErrorMessage(),
        left_effective_predicate->formatForErrorMessage(),
        right_effective_predicate->formatForErrorMessage(),
        join_predicate->formatForErrorMessage());

    if (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross)
    {
        InnerJoinResult inner_result = processInnerJoin(
            inherited_predicate, left_effective_predicate, right_effective_predicate, join_predicate, left_symbols, right_symbols);
        left_predicate = inner_result.left_predicate;
        right_predicate = inner_result.right_predicate;
        post_join_predicate = inner_result.post_join_predicate;
        new_join_predicate = inner_result.join_predicate;
    }
    else if (kind == ASTTableJoin::Kind::Left)
    {
        OuterJoinResult left_result = processOuterJoin(
            inherited_predicate, left_effective_predicate, right_effective_predicate, join_predicate, left_symbols, right_symbols);
        left_predicate = left_result.outer_predicate;
        right_predicate = left_result.inner_predicate;
        post_join_predicate = left_result.post_join_predicate;
        new_join_predicate = left_result.join_predicate;
    }
    else if (kind == ASTTableJoin::Kind::Right)
    {
        OuterJoinResult left_result = processOuterJoin(
            inherited_predicate, right_effective_predicate, left_effective_predicate, join_predicate, right_symbols, left_symbols);
        left_predicate = left_result.inner_predicate;
        right_predicate = left_result.outer_predicate;
        post_join_predicate = left_result.post_join_predicate;
        new_join_predicate = left_result.join_predicate;
    }
    else if (kind == ASTTableJoin::Kind::Full)
    {
        left_predicate = PredicateConst::TRUE_VALUE;
        right_predicate = PredicateConst::TRUE_VALUE;
        post_join_predicate = inherited_predicate;
        new_join_predicate = join_predicate;
    }
    else
    {
        throw Exception("Unsupported join type : Comma", ErrorCodes::NOT_IMPLEMENTED);
    }

    // Create identity projections for all existing symbols
    Assignments left_assignments;
    NameToType left_types;
    const DataStream & left_output = left->getStep()->getOutputStream();
    for (const auto & column : left_output.header)
    {
        left_assignments.emplace_back(column.name, std::make_shared<ASTIdentifier>(column.name));
        left_types[column.name] = column.type;
    }

    Assignments right_assignments;
    NameToType right_types;
    const DataStream & right_output = right->getStep()->getOutputStream();
    for (const auto & column : right_output.header)
    {
        right_assignments.emplace_back(column.name, std::make_shared<ASTIdentifier>(column.name));
        right_types[column.name] = column.type;
    }

    // Create new projections for the new join clauses
    std::set<std::pair<String, String>> join_clauses;
    std::vector<ConstASTPtr> join_filters;
    auto left_type_analyzer = TypeAnalyzer::create(context, left->getStep()->getOutputStream().header.getNamesAndTypes());
    auto right_type_analyzer = TypeAnalyzer::create(context, right->getStep()->getOutputStream().header.getNamesAndTypes());
    for (auto & conjunct : PredicateUtils::extractConjuncts(new_join_predicate))
    {
        if (PredicateUtils::isJoinClause(conjunct, left_symbols, right_symbols, context))
        {
            const auto & equality = conjunct->as<ASTFunction &>();
            std::set<std::string> left_equality_symbols = SymbolsExtractor::extract(equality.arguments->getChildren()[0]);
            std::set<std::string> right_equality_symbols = SymbolsExtractor::extract(equality.arguments->getChildren()[1]);
            bool left_aligned_comparison = SymbolUtils::containsAll(left_symbols, left_equality_symbols);
            bool right_aligned_comparison = SymbolUtils::containsAll(right_symbols, right_equality_symbols);
            ASTPtr & left_expression = (left_aligned_comparison && right_aligned_comparison) ? equality.arguments->getChildren()[0]
                                                                                             : equality.arguments->getChildren()[1];
            ASTPtr & right_expression = (left_aligned_comparison && right_aligned_comparison) ? equality.arguments->getChildren()[1]
                                                                                              : equality.arguments->getChildren()[0];
            String left_symbol = left_expression->as<ASTIdentifier>()
                ? left_expression->getColumnName()
                : context->getSymbolAllocator()->newSymbol(left_expression->getColumnName());
            if (!left_symbols.contains(left_symbol))
            {
                left_assignments.emplace_back(left_symbol, left_expression);
                left_types[left_symbol] = left_type_analyzer.getType(left_expression);
            }
            String right_symbol = right_expression->as<ASTIdentifier>()
                ? right_expression->getColumnName()
                : context->getSymbolAllocator()->newSymbol(right_expression->getColumnName());
            if (!right_symbols.contains(right_symbol))
            {
                right_assignments.emplace_back(right_symbol, right_expression);
                right_types[right_symbol] = right_type_analyzer.getType(right_expression);
            }
            join_clauses.emplace(std::make_pair(left_symbol, right_symbol));
        }
        else
        {
            join_filters.emplace_back(conjunct);
        }
    }

    ConstASTPtr left_implicit_filter = PredicateConst::TRUE_VALUE;
    ConstASTPtr right_implicit_filter = PredicateConst::TRUE_VALUE;

    auto build_implicit_filter = [](const Names & join_keys) {
        std::vector<ConstASTPtr> conjuncts;

        for (const auto & key : join_keys)
        {
            conjuncts.push_back(makeASTFunction("isNotNull", std::make_shared<ASTIdentifier>(key)));
        }

        return PredicateUtils::combineConjuncts(conjuncts);
    };
    // TODO: broaden this by consider SEMI JOIN & ANTI JOIN
    if (isRegularJoin(*step))
    {
        if (!isLeftOrFull(step->getKind()))
            left_implicit_filter = build_implicit_filter(step->getLeftKeys());

        if (!isRightOrFull(step->getKind()))
            right_implicit_filter = build_implicit_filter(step->getRightKeys());
    }

    if (kind == ASTTableJoin::Kind::Cross && !join_clauses.empty() /*inner join*/)
    {
        Names left_keys;
        Names right_keys;
        for (const auto & item : join_clauses)
        {
            left_keys.emplace_back(item.first);
            right_keys.emplace_back(item.second);
        }
        left_implicit_filter = build_implicit_filter(left_keys);
        right_implicit_filter = build_implicit_filter(right_keys);
    }

    // TODO: combine join implicit filter with inherited extra_predicate_for_simplify_outer_join
    PredicateContext left_context{
        .predicate = left_predicate, .extra_predicate_for_simplify_outer_join = left_implicit_filter, .context = predicate_context.context};
    PredicateContext right_context{
        .predicate = right_predicate,
        .extra_predicate_for_simplify_outer_join = right_implicit_filter,
        .context = predicate_context.context};
    PlanNodePtr left_source;
    PlanNodePtr right_source;

    LOG_DEBUG(
        &Poco::Logger::get("PredicateVisitor"),
        "join node {}, left context: {}, right context: {}",
        node.getId(),
        left_predicate->formatForErrorMessage(),
        right_predicate->formatForErrorMessage());

    bool join_clauses_unmodified = PredicateUtils::isJoinClauseUnmodified(join_clauses, step->getLeftKeys(), step->getRightKeys());
    if (!join_clauses_unmodified)
    {
        auto left_expression_step = std::make_shared<ProjectionStep>(left->getStep()->getOutputStream(), left_assignments, left_types);
        auto left_expression_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(left_expression_step), PlanNodes{left});

        auto right_expression_step = std::make_shared<ProjectionStep>(right->getStep()->getOutputStream(), right_assignments, right_types);
        auto right_expression_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(right_expression_step), PlanNodes{right});

        left_source = process(*left_expression_node, left_context);
        right_source = process(*right_expression_node, right_context);
    }
    else
    {
        left_source = process(*left, left_context);
        right_source = process(*right, right_context);
    }

    PlanNodePtr left_source_expression_node;
    if (Utils::isIdentity(left_assignments))
    {
        left_source_expression_node = left_source;
    }
    else
    {
        auto left_source_expression_step
            = std::make_shared<ProjectionStep>(left_source->getStep()->getOutputStream(), left_assignments, left_types);
        left_source_expression_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(left_source_expression_step), PlanNodes{left_source});
    }

    PlanNodePtr right_source_expression_node;
    if (Utils::isIdentity(right_assignments))
    {
        right_source_expression_node = right_source;
    }
    else
    {
        auto right_source_expression_step
            = std::make_shared<ProjectionStep>(right_source->getStep()->getOutputStream(), right_assignments, right_types);
        right_source_expression_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(right_source_expression_step), PlanNodes{right_source});
    }

    PlanNodePtr output_node = node.shared_from_this();

    const DataStream & left_data_stream = left_source_expression_node->getStep()->getOutputStream();
    const DataStream & right_data_stream = right_source_expression_node->getStep()->getOutputStream();

    DataStreams streams = {left_data_stream, right_data_stream};

    auto left_header = left_data_stream.header;
    auto right_header = right_data_stream.header;
    NamesAndTypes output = step->getOutputStream().header.getNamesAndTypes();
    // for (const auto & item : left_header)
    // {
    //     output.emplace_back(NameAndTypePair{item.name, item.type});
    // }
    // for (const auto & item : right_header)
    // {
    //     output.emplace_back(NameAndTypePair{item.name, item.type});
    // }

    // cast extracted join keys to super type
    Names left_keys;
    Names right_keys;
    {
        ProjectionPlanner left_planner(left_source_expression_node, context);
        ProjectionPlanner right_planner(right_source_expression_node, context);
        const bool allow_extended_type_conversion = context->getSettingsRef().allow_extended_type_conversion;
        const bool enable_implicit_arg_type_convert = context->getSettingsRef().enable_implicit_arg_type_convert;
        bool need_project = false;

        for (const auto & clause : join_clauses)
        {
            String left_key = clause.first;
            String right_key = clause.second;
            auto left_type = left_planner.getColumnType(left_key);
            auto right_type = right_planner.getColumnType(right_key);

            if (!JoinCommon::isJoinCompatibleTypes(left_type, right_type))
            {
                DataTypePtr common_type;
                try
                {
                    common_type
                        = getCommonType(DataTypes{left_type, right_type}, enable_implicit_arg_type_convert, allow_extended_type_conversion);
                }
                catch (DB::Exception & ex)
                {
                    throw Exception(
                        "Type mismatch of columns to JOIN by: " + left_type->getName() + " at left, " + right_type->getName()
                            + " at right. " + "Can't get supertype: " + ex.message(),
                        ErrorCodes::TYPE_MISMATCH);
                }
                left_key = left_planner.addColumn(makeCastFunction(std::make_shared<ASTIdentifier>(left_key), common_type)).first;
                right_key = right_planner.addColumn(makeCastFunction(std::make_shared<ASTIdentifier>(right_key), common_type)).first;
                need_project = true;
            }

            left_keys.emplace_back(left_key);
            right_keys.emplace_back(right_key);
        }

        // TODO: without `need_project`, addRuntimeFilter have a different result for ssb q10
        if (need_project)
        {
            left_source_expression_node = left_planner.build();
            right_source_expression_node = right_planner.build();
        }
    }

    ASTPtr new_join_filter = PredicateUtils::combineConjuncts(join_filters);

    // can not push normal filter into any inner join
    auto strictness = step->getStrictness();
    if (strictness != ASTTableJoin::Strictness::Unspecified && strictness != ASTTableJoin::Strictness::All
        && (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross)
        && (!step->getFilter() || PredicateUtils::isTruePredicate(step->getFilter())))
    {
        auto join_conj = PredicateUtils::extractConjuncts<ConstASTPtr>(new_join_filter);
        auto post_conj = PredicateUtils::extractConjuncts<ConstASTPtr>(post_join_predicate);
        join_conj.insert(join_conj.end(), post_conj.begin(), post_conj.end());
        post_join_predicate = PredicateUtils::combineConjuncts(join_conj);
        new_join_filter = PredicateConst::TRUE_VALUE;
    }

    std::shared_ptr<JoinStep> join_step;
    if (kind == ASTTableJoin::Kind::Cross)
    {
        join_step = std::make_shared<JoinStep>(
            streams,
            DataStream{.header = output},
            ASTTableJoin::Kind::Inner,
            ASTTableJoin::Strictness::All,
            step->getMaxStreams(),
            step->getKeepLeftReadInOrder(),
            left_keys,
            right_keys,
            std::vector<bool>{},
            new_join_filter,
            step->isHasUsing(),
            step->getRequireRightKeys(),
            step->getAsofInequality(),
            step->getDistributionType(),
            step->getJoinAlgorithm(),
            step->isMagic(),
            step->isOrdered(),
            step->isSimpleReordered(),
            step->getRuntimeFilterBuilders(),
            step->getHints());
    }
    else
    {
        join_step = std::make_shared<JoinStep>(
            streams,
            DataStream{.header = output},
            kind,
            step->getStrictness(),
            step->getMaxStreams(),
            step->getKeepLeftReadInOrder(),
            left_keys,
            right_keys,
            std::vector<bool>{},
            new_join_filter,
            step->isHasUsing(),
            step->getRequireRightKeys(),
            step->getAsofInequality(),
            step->getDistributionType(),
            step->getJoinAlgorithm(),
            step->isMagic(),
            step->isOrdered(),
            step->isSimpleReordered(),
            step->getRuntimeFilterBuilders(),
            step->getHints());
    }

    auto join_node = PlanNodeBase::createPlanNode(
        context->nextNodeId(), join_step, PlanNodes{left_source_expression_node, right_source_expression_node}, node.getStatistics());

    /**
     * Predicate push down may produce nest loop join with right join, which is not supported by nest loop join.
     * todo: remove this if hash join support filter or nest loop join support right join.
     */
    if (join_step->enforceNestLoopJoin() && join_step->supportSwap() && join_step->getKind() == ASTTableJoin::Kind::Right)
    {
        join_step = std::make_shared<JoinStep>(
            DataStreams{join_step->getInputStreams()[1], join_step->getInputStreams()[0]},
            join_step->getOutputStream(),
            ASTTableJoin::Kind::Left,
            join_step->getStrictness(),
            join_step->getMaxStreams(),
            join_step->getKeepLeftReadInOrder(),
            join_step->getRightKeys(),
            join_step->getLeftKeys(),
            join_step->getKeyIdsNullSafe(),
            join_step->getFilter(),
            join_step->isHasUsing(),
            join_step->getRequireRightKeys(),
            join_step->getAsofInequality(),
            join_step->getDistributionType(),
            join_step->getJoinAlgorithm(),
            join_step->isMagic(),
            step->isOrdered(),
            step->isSimpleReordered(),
            step->getRuntimeFilterBuilders(),
            step->getHints());
        join_node = PlanNodeBase::createPlanNode(
            context->nextNodeId(), join_step, PlanNodes{right_source_expression_node, left_source_expression_node}, node.getStatistics());
    }

    if (!PredicateUtils::isTruePredicate(post_join_predicate))
    {
        auto filter_step = std::make_shared<FilterStep>(join_node->getStep()->getOutputStream(), post_join_predicate);
        auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{join_node});
        output_node = filter_node;
    }
    else
    {
        output_node = join_node;
    }

    NamesAndTypes join_node_output = node.getStep()->getOutputStream().header.getNamesAndTypes();
    NamesAndTypes output_node_output = output_node->getStep()->getOutputStream().header.getNamesAndTypes();
    if (join_node_output != output_node_output)
    {
        Assignments output_assignments;
        NameToType output_types;
        for (auto & column : output_node_output)
        {
            Assignment output_assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            output_assignments.emplace_back(output_assignment);
            output_types[column.name] = column.type;
        }
        auto output_expression_step
            = std::make_shared<ProjectionStep>(output_node->getStep()->getOutputStream(), std::move(output_assignments), std::move(output_types));
        auto output_expression_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(output_expression_step), PlanNodes{output_node});
        output_node = output_expression_node;
    }
    return output_node;
}

PlanNodePtr PredicateVisitor::visitArrayJoinNode(ArrayJoinNode & node, PredicateContext & predicate_context)
{
    const auto & step = *node.getStep();

    ConstASTPtr inherited_predicate = predicate_context.predicate;
    EqualityInference equality_inference = EqualityInference::newInstance(inherited_predicate, context);

    // filter with array join result column, can't push down
    std::vector<String> array_join_columns;
    for (const auto & input : step.getResultNameSet())
    {
        array_join_columns.emplace_back(input);
    }

    std::vector<ConstASTPtr> pushdown_array_join_conjuncts;
    std::vector<ConstASTPtr> post_array_join_conjuncts;

    for (auto & conjunct : PredicateUtils::extractConjuncts(inherited_predicate))
    {
        // Strip out non-deterministic conjuncts
        if (!ExpressionDeterminism::isDeterministic(conjunct, context))
        {
            post_array_join_conjuncts.emplace_back(conjunct);
            continue;
        }

        /// for predicate contains array join column, can't preform push down.
        std::set<String> predicate_symbols = SymbolsExtractor::extract(conjunct);
        if (PredicateUtils::containsAny(array_join_columns, predicate_symbols))
        {
            post_array_join_conjuncts.emplace_back(conjunct);
        }
        else
        {
            pushdown_array_join_conjuncts.emplace_back(conjunct);
        }
    }

    PredicateContext array_join_context{
        .predicate = PredicateUtils::combineConjuncts(pushdown_array_join_conjuncts),
        .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE,
        .context = predicate_context.context};
    PlanNodePtr rewritten = process(*node.getChildren()[0], array_join_context);

    PlanNodePtr output = node.shared_from_this();
    if (rewritten != node.getChildren()[0])
    {
        output = PlanNodeBase::createPlanNode(context->nextNodeId(), node.getStep(), PlanNodes{rewritten}, node.getStatistics());
    }
    if (!post_array_join_conjuncts.empty())
    {
        auto filter_step = std::make_shared<FilterStep>(
            output->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(post_array_join_conjuncts));
        output = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{output});
    }
    return output;
}

PlanNodePtr PredicateVisitor::visitExchangeNode(ExchangeNode & node, PredicateContext & predicate_context)
{
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitWindowNode(WindowNode & node, PredicateContext & predicate_context)
{
    auto & step_ptr = node.getStep();
    const auto * step = dynamic_cast<const WindowStep *>(step_ptr.get());

    const WindowDescription & window_desc = step->getWindow();
    SortDescription scheme = window_desc.partition_by;
    Strings partition_symbols;
    for (auto & partition : scheme)
    {
        partition_symbols.emplace_back(partition.column_name);
    }

    auto predicate = predicate_context.predicate;
    ContextMutablePtr ctx = predicate_context.context;
    auto conjuncts = PredicateUtils::extractConjuncts(predicate);

    /// guarantee the predicates which can be pushed down through window step
    /// are deterministic functions, and belongs to window partitioning symbols.
    std::vector<ConstASTPtr> push_down_conjuncts;
    std::vector<ConstASTPtr> non_push_down_conjuncts;
    for (auto & conjunct : conjuncts)
    {
        std::set<String> unique_symbols = SymbolsExtractor::extract(conjunct);
        if (ExpressionDeterminism::isDeterministic(conjunct, ctx) && PredicateUtils::containsAll(partition_symbols, unique_symbols))
        {
            push_down_conjuncts.emplace_back(conjunct);
        }
        else
        {
            non_push_down_conjuncts.emplace_back(conjunct);
        }
    }

    PredicateContext window_context{
        .predicate = PredicateUtils::combineConjuncts(push_down_conjuncts),
        .extra_predicate_for_simplify_outer_join = predicate_context.extra_predicate_for_simplify_outer_join,
        .context = ctx};
    PlanNodePtr rewritten = processChild(node, window_context);

    ASTPtr non_push_down_predicate = PredicateUtils::combineConjuncts(non_push_down_conjuncts);
    if (!PredicateUtils::isTruePredicate(non_push_down_predicate))
    {
        ASTPtr extra_predicate = PredicateUtils::combineConjuncts(non_push_down_conjuncts);
        auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), extra_predicate);
        auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
        return filter_node;
    }
    return rewritten;
}

PlanNodePtr PredicateVisitor::visitMarkDistinctNode(MarkDistinctNode & node, PredicateContext & predicate_context)
{
    auto & step_ptr = node.getStep();
    auto & step = dynamic_cast<MarkDistinctStep &>(*step_ptr);

    const Strings & distinct_symbols = step.getDistinctSymbols();

    auto predicate = predicate_context.predicate;
    ContextMutablePtr ctx = predicate_context.context;
    auto conjuncts = PredicateUtils::extractConjuncts(predicate);

    /// guarantee the predicates which can be pushed down through mark distinct step
    /// are deterministic functions, and belongs to distinct symbols.
    std::vector<ConstASTPtr> push_down_conjuncts;
    std::vector<ConstASTPtr> non_push_down_conjuncts;
    for (auto & conjunct : conjuncts)
    {
        std::set<String> unique_symbols = SymbolsExtractor::extract(conjunct);
        if (ExpressionDeterminism::isDeterministic(conjunct, ctx) && PredicateUtils::containsAll(distinct_symbols, unique_symbols))
        {
            push_down_conjuncts.emplace_back(conjunct);
        }
        else
        {
            non_push_down_conjuncts.emplace_back(conjunct);
        }
    }

    PredicateContext window_context{
        .predicate = PredicateUtils::combineConjuncts(push_down_conjuncts),
        .extra_predicate_for_simplify_outer_join = predicate_context.extra_predicate_for_simplify_outer_join,
        .context = ctx};
    PlanNodePtr rewritten = processChild(node, window_context);

    ASTPtr non_push_down_predicate = PredicateUtils::combineConjuncts(non_push_down_conjuncts);
    if (!PredicateUtils::isTruePredicate(non_push_down_predicate))
    {
        ASTPtr extra_predicate = PredicateUtils::combineConjuncts(non_push_down_conjuncts);
        auto filter_step = std::make_shared<FilterStep>(rewritten->getStep()->getOutputStream(), extra_predicate);
        auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{rewritten});
        return filter_node;
    }
    return rewritten;
}

PlanNodePtr PredicateVisitor::visitMergeSortingNode(MergeSortingNode & node, PredicateContext & predicate_context)
{
    if (node.getStep()->getLimit() != 0)
        return visitPlanNode(node, predicate_context);
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitPartialSortingNode(PartialSortingNode & node, PredicateContext & predicate_context)
{
    if (node.getStep()->getLimit() != 0)
        return visitPlanNode(node, predicate_context);
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitSortingNode(SortingNode & node, PredicateContext & predicate_context)
{
    if (node.getStep()->hasPreparedParam() || node.getStep()->getLimitValue() != 0)
        return visitPlanNode(node, predicate_context);
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitUnionNode(UnionNode & node, PredicateContext & predicate_context)
{
    const auto & step = *node.getStep();
    PlanNodes children;
    ConstASTPtr predicate = predicate_context.predicate;
    for (size_t i = 0; i < node.getChildren().size(); i++)
    {
        Assignments assignments;
        for (const auto & output_to_input : step.getOutToInputs())
        {
            assignments.emplace_back(output_to_input.first, std::make_shared<ASTIdentifier>(output_to_input.second[i]));
        }
        ASTPtr source_predicate = ExpressionInliner::inlineSymbols(predicate, assignments);
        ASTPtr source_extra_predicate
            = ExpressionInliner::inlineSymbols(predicate_context.extra_predicate_for_simplify_outer_join, assignments);
        PredicateContext source_context{
            .predicate = source_predicate,
            .extra_predicate_for_simplify_outer_join = source_extra_predicate,
            .context = predicate_context.context};
        PlanNodePtr child = process(*node.getChildren()[i], source_context);
        children.emplace_back(child);
    }
    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr PredicateVisitor::visitDistinctNode(DistinctNode & node, PredicateContext & predicate_context)
{
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode & node, PredicateContext & predicate_context)
{
    std::set<String> predicate_symbols = SymbolsExtractor::extract(predicate_context.predicate);
    const auto & step = *node.getStep();
    if (predicate_symbols.contains(step.getName()))
    {
        throw Exception("UniqueId in predicate is not yet supported", ErrorCodes::NOT_IMPLEMENTED);
    }
    return processChild(node, predicate_context);
}

PlanNodePtr PredicateVisitor::visitCTERefNode(CTERefNode & node, PredicateContext & predicate_context)
{
    auto * cte_step = dynamic_cast<CTERefStep *>(node.getStep().get());
    if (!cte_step->hasFilter() && !PredicateUtils::isTruePredicate(predicate_context.predicate))
        cte_step->setFilter(true);

    auto & cte_refs = cte_predicates[cte_step->getId()];
    cte_refs.emplace_back(std::make_pair(cte_step, predicate_context.predicate));
    // only push filter through cte when filters exist above all cte, and only push filter in the first time.
    if (cte_refs.size() == cte_reference_counts.at(cte_step->getId()))
    {
        auto & cte_def = cte_info.getCTEDef(cte_step->getId());
        if (pushdown_filter_into_cte)
        {
            std::vector<ConstASTPtr> common_filters;
            for (const auto & cte_ref : cte_refs)
            {
                auto mapping = cte_ref.first->getOutputColumns();
                auto mapper = SymbolMapper::simpleMapper(mapping);
                auto mapped_filter = mapper.map(cte_ref.second);
                common_filters.emplace_back(mapped_filter);
            }
            auto optimized_expression = CommonPredicatesRewriter::rewrite(PredicateUtils::combineDisjuncts(common_filters), context);
            optimized_expression = ExpressionInterpreter::optimizePredicate(
                optimized_expression, cte_def->getStep()->getOutputStream().header.getNamesToTypes(), context);
            PredicateContext cte_predicate_context{
                .predicate = optimized_expression,
                .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE,
                .context = predicate_context.context};
            cte_def = VisitorUtil::accept(cte_def, *this, cte_predicate_context);
        }
        else
        {
            PredicateContext cte_predicate_context{
                .predicate = PredicateConst::TRUE_VALUE,
                .extra_predicate_for_simplify_outer_join = PredicateConst::TRUE_VALUE,
                .context = predicate_context.context};
            cte_def = VisitorUtil::accept(cte_def, *this, cte_predicate_context);
        }
    }

    return visitPlanNode(node, predicate_context);
}

PlanNodePtr PredicateVisitor::process(PlanNodeBase & node, PredicateContext & predicate_context)
{
    LOG_TRACE(logger, "node {} has inherited predicate: {}", node.getId(), predicate_context.predicate->formatForErrorMessage());
    return VisitorUtil::accept(node, *this, predicate_context);
}

PlanNodePtr PredicateVisitor::processChild(PlanNodeBase & node, PredicateContext & predicate_context)
{
    if (node.getChildren().empty())
        return node.shared_from_this();

    PlanNodes children;
    for (const auto & item : node.getChildren())
    {
        PlanNodePtr child = process(*item, predicate_context);
        children.emplace_back(child);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

InnerJoinResult PredicateVisitor::processInnerJoin(
    ConstASTPtr & inherited_predicate,
    ConstASTPtr & left_predicate,
    ConstASTPtr & right_predicate,
    ConstASTPtr & join_predicate,
    std::set<String> & left_symbols,
    std::set<String> & right_symbols)
{
    std::vector<ConstASTPtr> left_conjuncts;
    std::vector<ConstASTPtr> right_conjuncts;
    std::vector<ConstASTPtr> join_conjuncts;

    for (auto & predicate : PredicateUtils::extractConjuncts(inherited_predicate))
    {
        if (!ExpressionDeterminism::isDeterministic(predicate, context))
        {
            join_conjuncts.emplace_back(predicate);
        }
    }
    // Strip out non-deterministic conjuncts
    inherited_predicate = ExpressionDeterminism::filterDeterministicConjuncts(inherited_predicate, context);

    for (auto & predicate : PredicateUtils::extractConjuncts(join_predicate))
    {
        if (!ExpressionDeterminism::isDeterministic(predicate, context))
        {
            join_conjuncts.emplace_back(predicate);
        }
    }
    join_predicate = ExpressionDeterminism::filterDeterministicConjuncts(join_predicate, context);
    left_predicate = ExpressionDeterminism::filterDeterministicConjuncts(left_predicate, context);
    right_predicate = ExpressionDeterminism::filterDeterministicConjuncts(right_predicate, context);

    // Attempt to simplify the effective left/right predicates with the predicate we're pushing down
    // This, effectively, inlines any constants derived from such predicate
    std::set<String> left_scope;
    for (const auto & symbol : left_symbols)
    {
        left_scope.emplace(symbol);
    }
    std::set<String> right_scope;
    for (const auto & symbol : right_symbols)
    {
        right_scope.emplace(symbol);
    }
    EqualityInference predicate_inference = EqualityInference::newInstance(inherited_predicate, context);
    ASTPtr simplified_left_predicate = predicate_inference.rewrite(left_predicate, left_scope);
    ASTPtr simplified_right_predicate = predicate_inference.rewrite(right_predicate, right_scope);

    LOG_TRACE(logger, "predicate_inference : {}", predicate_inference.toString());

    // simplify predicate based on known equalities guaranteed by the left/right side
    EqualityInference assertions = EqualityInference::newInstance(std::vector<ConstASTPtr>{left_predicate, right_predicate}, context);

    LOG_TRACE(logger, "assertions : {}", assertions.toString());

    std::set<String> union_scope;
    std::set_union(
        left_scope.begin(), left_scope.end(), right_scope.begin(), right_scope.end(), std::inserter(union_scope, union_scope.end()));
    inherited_predicate = assertions.rewrite(inherited_predicate, union_scope);

    if (!inherited_predicate)
        throw Exception("Unexpected error in predicate pushdown", ErrorCodes::PLAN_BUILD_ERROR);

    // Generate equality inferences
    EqualityInference all_inference = EqualityInference::newInstance(
        std::vector<ConstASTPtr>{
            inherited_predicate, left_predicate, right_predicate, join_predicate, simplified_left_predicate, simplified_right_predicate},
        context);
    EqualityInference all_inference_without_left_inferred = EqualityInference::newInstance(
        std::vector<ConstASTPtr>{inherited_predicate, right_predicate, join_predicate, simplified_right_predicate}, context);
    EqualityInference all_inference_without_right_inferred = EqualityInference::newInstance(
        std::vector<ConstASTPtr>{inherited_predicate, left_predicate, join_predicate, simplified_left_predicate}, context);

    LOG_TRACE(logger, "all_inference : {}", all_inference.toString());
    LOG_TRACE(logger, "all_inference_without_left_inferred : {}", all_inference_without_left_inferred.toString());
    LOG_TRACE(logger, "all_inference_without_right_inferred : {}", all_inference_without_right_inferred.toString());

    // Add equalities from the inference back in
    auto equalities = all_inference.partitionedBy(left_scope).getScopeStraddlingEqualities();
    auto equalities_without_left = all_inference_without_left_inferred.partitionedBy(left_scope).getScopeEqualities();
    auto equalities_without_right = all_inference_without_right_inferred.partitionedBy(right_scope).getScopeEqualities();

    left_conjuncts.reserve(equalities_without_left.size());
    right_conjuncts.reserve(equalities_without_right.size());
    for (auto & equality_without_left : equalities_without_left)
    {
        left_conjuncts.emplace_back(equality_without_left);
    }
    for (auto & equality_without_right : equalities_without_right)
    {
        right_conjuncts.emplace_back(equality_without_right);
    }
    for (auto & equality : equalities)
    {
        join_conjuncts.emplace_back(equality);
    }

    // Sort through conjuncts in inheritedPredicate that were not used for inference
    auto non_inferrable_conjuncts = EqualityInference::nonInferrableConjuncts(inherited_predicate, context);
    for (auto & non_inferrable_conjunct : non_inferrable_conjuncts)
    {
        ASTPtr left_rewritten_conjunct = all_inference.rewrite(non_inferrable_conjunct, left_scope);
        if (left_rewritten_conjunct != nullptr)
        {
            // as left_rewritten_conjunct will be modified, so clone here.
            left_conjuncts.emplace_back(left_rewritten_conjunct->clone());
        }

        ASTPtr right_rewritten_conjunct = all_inference.rewrite(non_inferrable_conjunct, right_scope);
        if (right_rewritten_conjunct != nullptr)
        {
            right_conjuncts.emplace_back(right_rewritten_conjunct->clone());
        }

        // Drop predicate after join only if unable to push down to either side
        if (left_rewritten_conjunct == nullptr && right_rewritten_conjunct == nullptr)
        {
            join_conjuncts.emplace_back(non_inferrable_conjunct);
        }
    }

    // See if we can push the right effective predicate to the left side
    auto right_non_inferrable_conjuncts = EqualityInference::nonInferrableConjuncts(simplified_right_predicate, context);
    for (auto & conjunct : right_non_inferrable_conjuncts)
    {
        ASTPtr rewritten = all_inference.rewrite(conjunct, left_scope);
        if (rewritten != nullptr)
        {
            left_conjuncts.emplace_back(rewritten);
        }
    }

    // See if we can push the left effective predicate to the right side
    auto left_non_inferrable_conjuncts = EqualityInference::nonInferrableConjuncts(simplified_left_predicate, context);
    for (auto & conjunct : left_non_inferrable_conjuncts)
    {
        ASTPtr rewritten = all_inference.rewrite(conjunct, right_scope);
        if (rewritten != nullptr)
        {
            right_conjuncts.emplace_back(rewritten);
        }
    }

    // See if we can push any parts of the join predicates to either side
    auto join_non_inferrable_conjuncts = EqualityInference::nonInferrableConjuncts(join_predicate, context);
    for (auto & conjunct : join_non_inferrable_conjuncts)
    {
        ASTPtr left_rewritten = all_inference.rewrite(conjunct, left_scope);
        if (left_rewritten != nullptr)
        {
            left_conjuncts.emplace_back(left_rewritten->clone());
        }

        ASTPtr right_rewritten = all_inference.rewrite(conjunct, right_scope);
        if (right_rewritten != nullptr)
        {
            right_conjuncts.emplace_back(right_rewritten->clone());
        }

        if (left_rewritten == nullptr && right_rewritten == nullptr)
        {
            join_conjuncts.emplace_back(conjunct);
        }
    }

    return InnerJoinResult{
        PredicateUtils::combineConjuncts(left_conjuncts),
        PredicateUtils::combineConjuncts(right_conjuncts),
        PredicateUtils::combineConjuncts(join_conjuncts),
        PredicateConst::TRUE_VALUE};
}

OuterJoinResult PredicateVisitor::processOuterJoin(
    ConstASTPtr & inherited_predicate,
    ConstASTPtr & outer_predicate,
    ConstASTPtr & inner_predicate,
    ConstASTPtr & join_predicate,
    std::set<String> & outer_symbols,
    std::set<String> & inner_symbols)
{
    std::vector<ConstASTPtr> outer_pushdown_conjuncts;
    std::vector<ConstASTPtr> inner_pushdown_conjuncts;
    std::vector<ConstASTPtr> post_join_conjuncts;
    std::vector<ConstASTPtr> join_conjuncts;

    // Strip out non-deterministic conjuncts
    auto predicates = PredicateUtils::extractConjuncts(inherited_predicate);
    for (auto & pre : predicates)
    {
        if (!ExpressionDeterminism::isDeterministic(pre, context))
        {
            post_join_conjuncts.emplace_back(pre);
        }
    }

    inherited_predicate = ExpressionDeterminism::filterDeterministicConjuncts(inherited_predicate, context);
    outer_predicate = ExpressionDeterminism::filterDeterministicConjuncts(outer_predicate, context);
    inner_predicate = ExpressionDeterminism::filterDeterministicConjuncts(inner_predicate, context);

    std::vector<ConstASTPtr> join_predicates = PredicateUtils::extractConjuncts(join_predicate);
    for (auto & pre : join_predicates)
    {
        if (!ExpressionDeterminism::isDeterministic(pre, context))
        {
            join_conjuncts.emplace_back(pre);
        }
    }

    join_predicate = ExpressionDeterminism::filterDeterministicConjuncts(join_predicate, context);

    // Generate equality inferences
    EqualityInference inherited_inference = EqualityInference::newInstance(inherited_predicate, context);
    EqualityInference outer_inference
        = EqualityInference::newInstance(std::vector<ConstASTPtr>{inherited_predicate, outer_predicate}, context);

    LOG_TRACE(logger, "inherited_inference : {}", inherited_inference.toString());
    LOG_TRACE(logger, "outer_inference : {}", outer_inference.toString());

    EqualityPartition equality_partition = inherited_inference.partitionedBy(outer_symbols);

    LOG_TRACE(logger, "equality_partition : {}", equality_partition.toString());

    const auto & scope_equalities = equality_partition.getScopeEqualities();
    auto outer_only_inherited_equalities = PredicateUtils::combineConjuncts(scope_equalities);
    EqualityInference potential_null_symbol_inference = EqualityInference::newInstance(
        std::vector<ConstASTPtr>{outer_only_inherited_equalities, outer_predicate, inner_predicate, join_predicate}, context);

    LOG_TRACE(logger, "potential_null_symbol_inference : {}", potential_null_symbol_inference.toString());

    // Push outer and join equalities into the inner side. For example:
    // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

    EqualityInference potential_null_symbol_inference_without_inner_inferred = EqualityInference::newInstance(
        std::vector<ConstASTPtr>{outer_only_inherited_equalities, outer_predicate, join_predicate}, context);

    LOG_TRACE(
        logger,
        "potential_null_symbol_inference_without_inner_inferred : {}",
        potential_null_symbol_inference_without_inner_inferred.toString());

    EqualityPartition potential_null_symbol_inference_without_inner_inferred_partition
        = potential_null_symbol_inference_without_inner_inferred.partitionedBy(inner_symbols);

    LOG_TRACE(
        logger,
        "potential_null_symbol_inference_without_inner_inferred_partition : {}",
        potential_null_symbol_inference_without_inner_inferred_partition.toString());

    for (const auto & conjunct : potential_null_symbol_inference_without_inner_inferred_partition.getScopeEqualities())
    {
        inner_pushdown_conjuncts.emplace_back(conjunct);
    }

    // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
    EqualityPartition join_equality_partition = EqualityInference::newInstance(join_predicate, context).partitionedBy(inner_symbols);

    LOG_TRACE(logger, "join_equality_partition : {}", join_equality_partition.toString());

    for (const auto & conjunct : join_equality_partition.getScopeEqualities())
    {
        inner_pushdown_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : join_equality_partition.getScopeComplementEqualities())
    {
        join_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : join_equality_partition.getScopeStraddlingEqualities())
    {
        join_conjuncts.emplace_back(conjunct);
    }

    // Add the equalities from the inferences back in
    for (const auto & conjunct : equality_partition.getScopeEqualities())
    {
        outer_pushdown_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : equality_partition.getScopeComplementEqualities())
    {
        post_join_conjuncts.emplace_back(conjunct);
    }
    for (const auto & conjunct : equality_partition.getScopeStraddlingEqualities())
    {
        post_join_conjuncts.emplace_back(conjunct);
    }

    // See if we can push inherited predicates down
    for (const auto & conjunct : EqualityInference::nonInferrableConjuncts(inherited_predicate, context))
    {
        ASTPtr outer_rewritten = outer_inference.rewrite(conjunct, outer_symbols);
        if (outer_rewritten != nullptr)
        {
            outer_pushdown_conjuncts.emplace_back(outer_rewritten->clone());

            // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
            ASTPtr inner_rewritten = potential_null_symbol_inference.rewrite(outer_rewritten, inner_symbols);
            if (inner_rewritten != nullptr)
            {
                inner_pushdown_conjuncts.emplace_back(inner_rewritten);
            }
        }
        else
        {
            post_join_conjuncts.emplace_back(conjunct);
        }
    }

    // See if we can push down any outer effective predicates to the inner side
    for (const auto & conjunct : EqualityInference::nonInferrableConjuncts(outer_predicate, context))
    {
        auto rewritten = potential_null_symbol_inference.rewrite(conjunct, inner_symbols);
        if (rewritten != nullptr)
        {
            inner_pushdown_conjuncts.emplace_back(rewritten);
        }
    }

    // See if we can push down join predicates to the inner side
    for (const auto & conjunct : EqualityInference::nonInferrableConjuncts(join_predicate, context))
    {
        ASTPtr inner_rewritten = potential_null_symbol_inference.rewrite(conjunct, inner_symbols);
        if (inner_rewritten != nullptr)
        {
            inner_pushdown_conjuncts.emplace_back(inner_rewritten);
        }
        else
        {
            join_conjuncts.emplace_back(conjunct);
        }
    }

    return OuterJoinResult{
        PredicateUtils::combineConjuncts(outer_pushdown_conjuncts),
        PredicateUtils::combineConjuncts(inner_pushdown_conjuncts),
        PredicateUtils::combineConjuncts(join_conjuncts),
        PredicateUtils::combineConjuncts(post_join_conjuncts)};
}

void PredicateVisitor::tryNormalizeOuterToInnerJoin(JoinNode & node, const ConstASTPtr & inherited_predicate, ContextMutablePtr context)
{
    using Kind = ASTTableJoin::Kind;
    using Strictness = ASTTableJoin::Strictness;

    auto & step = *node.getStep();
    Kind kind = step.getKind();
    Strictness strictness = step.getStrictness();

    if (kind != Kind::Left && kind != Kind::Right && kind != Kind::Full)
        return;

    // TODO: ANTI JOINs also can be optimized
    // left any join CANNOT be converted to inner any join
    if (strictness != Strictness::All)
        return;

    auto column_types = step.getOutputStream().header.getNamesToTypes();

    auto build_symbols = [&](const PlanNodePtr & source) {
        std::unordered_map<String, Field> result;

        // note we cannot use type information of `source` node as outer join will change the column types of non-outer side
        for (const auto & name : source->getStep()->getOutputStream().header.getNames())
            if (column_types.count(name))
                result.emplace(name, column_types.at(name)->getDefault());

        return result;
    };

    std::unordered_map<String, Field> left_symbols = build_symbols(node.getChildren()[0]);
    std::unordered_map<String, Field> right_symbols = build_symbols(node.getChildren()[1]);

    if (isRightOrFull(kind) && canConvertOuterToInner(left_symbols, inherited_predicate, context, column_types))
    {
        kind = useInnerForRightSide(kind);
    }

    if (isLeftOrFull(kind) && canConvertOuterToInner(right_symbols, inherited_predicate, context, column_types))
    {
        kind = useInnerForLeftSide(kind);
    }

    step.setKind(kind);
}

bool PredicateVisitor::canConvertOuterToInner(
    const std::unordered_map<String, Field> & inner_symbols_for_outer_join,
    const ConstASTPtr & inherited_predicate,
    ContextMutablePtr context,
    const NameToType & column_types)
{
    auto interpreter = ExpressionInterpreter::optimizedInterpreter(column_types, inner_symbols_for_outer_join, context);
    for (auto & conjunct : PredicateUtils::extractConjuncts(inherited_predicate))
    {
        auto result = interpreter.evaluateConstantExpression(conjunct);
        if (result.has_value())
        {
            auto & val = result->second;
            if (val.isNull() || !applyVisitor(FieldVisitorConvertToNumber<bool>(), val))
                return true;
        }
    }
    return false;
}

ASTTableJoin::Kind PredicateVisitor::useInnerForLeftSide(ASTTableJoin::Kind kind)
{
    Utils::checkArgument(kind == ASTTableJoin::Kind::Full || kind == ASTTableJoin::Kind::Left);
    return kind == ASTTableJoin::Kind::Full ? ASTTableJoin::Kind::Right : ASTTableJoin::Kind::Inner;
}

ASTTableJoin::Kind PredicateVisitor::useInnerForRightSide(ASTTableJoin::Kind kind)
{
    Utils::checkArgument(kind == ASTTableJoin::Kind::Full || kind == ASTTableJoin::Kind::Right);
    return kind == ASTTableJoin::Kind::Full ? ASTTableJoin::Kind::Left : ASTTableJoin::Kind::Inner;
}

bool PredicateVisitor::isRegularJoin(const JoinStep & step)
{
    auto strictness = step.getStrictness();

    return isAll(strictness) || isAny(strictness);
}

ASTPtr EffectivePredicateExtractor::extract(PlanNodePtr & node, ContextMutablePtr & context)
{
    EffectivePredicateVisitor visitor{};
    return VisitorUtil::accept(node, visitor, context);
}

ASTPtr EffectivePredicateExtractor::extract(PlanNodeBase & node, ContextMutablePtr & context)
{
    EffectivePredicateVisitor visitor{};
    return VisitorUtil::accept(node, visitor, context);
}

ASTPtr EffectivePredicateVisitor::visitPlanNode(PlanNodeBase &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}

ASTPtr EffectivePredicateVisitor::visitLimitNode(LimitNode & node, ContextMutablePtr & context)
{
    return process(node, context);
}

ASTPtr EffectivePredicateVisitor::visitEnforceSingleRowNode(EnforceSingleRowNode & node, ContextMutablePtr & context)
{
    // EnforceSingleRowNode will produce at lease one row, can't propagate predicate.
    return visitPlanNode(node, context);
}

ASTPtr EffectivePredicateVisitor::visitProjectionNode(ProjectionNode & node, ContextMutablePtr & context)
{
    // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

    // Clear predicates involving symbols which are keys to non-identity assignments.
    // Assignment such as `s -> x + 1` establishes new semantics for symbol `s`.
    // If symbol `s` was present is the source plan and was included in underlying predicate, the predicate is no more valid.
    // Also, if symbol `s` is present in a project assignment's value, e.g. `s1 -> s + 1`, this assignment should't be used to derive equality.

    ASTPtr underlying_predicate = process(node, context);

    auto & step_ptr = *node.getStep();

    Assignments non_identity_assignments;
    std::set<String> newly_assigned_symbols;
    for (const auto & assignment : step_ptr.getAssignments())
    {
        String symbol = assignment.first;
        auto value = assignment.second;
        if (value->as<ASTIdentifier>() && value->getColumnName() == symbol)
            continue;

        // skip NULL
        if (value->as<ASTLiteral>() && value->as<ASTLiteral &>().value.isNull())
            continue;

        // skip id_1 := cast(id, SomeType)
        if (const auto * func = value->as<ASTFunction>())
            if (Poco::toLower(func->name) == "cast" && func->arguments->children[0]->as<ASTIdentifier>())
                continue;

        non_identity_assignments.emplace_back(assignment);
        newly_assigned_symbols.emplace(symbol);
    }

    std::vector<ConstASTPtr> valid_underlying_equalities;
    for (auto & conjunct : PredicateUtils::extractConjuncts(underlying_predicate))
    {
        std::set<String> uniques = SymbolsExtractor::extract(conjunct);
        std::set<String> symbols_to_propagate;
        std::set_intersection(
            uniques.begin(),
            uniques.end(),
            newly_assigned_symbols.begin(),
            newly_assigned_symbols.end(),
            std::inserter(symbols_to_propagate, symbols_to_propagate.begin()));
        if (symbols_to_propagate.empty())
        {
            valid_underlying_equalities.emplace_back(conjunct);
        }
    }
    std::vector<ConstASTPtr> projection_equalities;
    for (auto & assignment : non_identity_assignments)
    {
        std::set<String> uniques = SymbolsExtractor::extract(assignment.second);
        std::set<String> symbols_to_propagate;
        std::set_intersection(
            uniques.begin(),
            uniques.end(),
            newly_assigned_symbols.begin(),
            newly_assigned_symbols.end(),
            std::inserter(symbols_to_propagate, symbols_to_propagate.begin()));
        if (symbols_to_propagate.empty())
        {
            auto fun = makeASTFunction("equals", std::make_shared<ASTIdentifier>(assignment.first), assignment.second->clone());
            projection_equalities.emplace_back(fun);
        }
    }

    projection_equalities.insert(projection_equalities.end(), valid_underlying_equalities.begin(), valid_underlying_equalities.end());

    std::vector<String> output_symbols;
    for (const auto & output : step_ptr.getOutputStream().header)
    {
        output_symbols.emplace_back(output.name);
    }

    auto expression = PredicateUtils::combineConjuncts(projection_equalities);
    return pullExpressionThroughSymbols(expression, output_symbols, context);
}

ASTPtr EffectivePredicateVisitor::visitFilterNode(FilterNode & node, ContextMutablePtr & context)
{
    ASTPtr underlying_predicate = process(node, context);

    const auto & step = *node.getStep();
    auto predicate = step.getFilter();

    // Remove non-deterministic conjuncts
    predicate = ExpressionDeterminism::filterDeterministicConjuncts(predicate, context);

    // Remove dynamic filters
    std::vector<ConstASTPtr> predicates = PredicateUtils::extractConjuncts(predicate);
    std::vector<ConstASTPtr> removed_dynamic_filters;
    for (auto & pre : predicates)
    {
        if (!RuntimeFilterUtils::isInternalRuntimeFilter(pre))
        {
            removed_dynamic_filters.emplace_back(pre);
        }
    }

    /**
     * Disable extract predicate with inconsistent type.
     *
     * for predicate : expr#toDate('2023-06-29') = expr#'2023-06-29',
     * left argument and right argument both ASTIdentifier, but they
     * have different type, left type is Date, right type is String.
     */
    const NameToType & name_types = step.getOutputStream().getNamesToTypes();
    auto is_inconsistent = [&](ConstASTPtr & ptr) {
        if (ptr->as<ASTFunction>())
        {
            const auto & fun = ptr->as<ASTFunction &>();
            if (fun.arguments->children.size() == 2)
            {
                auto left = fun.arguments->getChildren()[0];
                auto right = fun.arguments->getChildren()[1];
                if (left->as<ASTIdentifier>() && right->as<ASTIdentifier>())
                {
                    auto left_type = name_types.at(left->getColumnName());
                    auto right_type = name_types.at(right->getColumnName());
                    return left_type->getName() != right_type->getName();
                }
            }
        }
        return false;
    };

    std::vector<ConstASTPtr> removed_inconsistent_type_filters;
    for (auto & ptr : removed_dynamic_filters)
    {
        if (is_inconsistent(ptr))
            continue;
        removed_inconsistent_type_filters.emplace_back(ptr);
    }

    std::vector<ConstASTPtr> removed_large_in_value_list
        = FilterStep::removeLargeInValueList(removed_inconsistent_type_filters, context->getSettingsRef().max_in_value_list_to_pushdown);

    // Adds on underlying_predicate
    removed_large_in_value_list.emplace_back(underlying_predicate);
    return PredicateUtils::combineConjuncts(removed_large_in_value_list);
}

ASTPtr EffectivePredicateVisitor::visitAggregatingNode(AggregatingNode & node, ContextMutablePtr & context)
{
    // GROUP BY () always produces a group, regardless of whether there's any
    // input (unlike the case where there are group by keys, which produce
    // no output if there's no input).
    // Therefore, we can't say anything about the effective predicate of the
    // output of such an aggregation.
    const auto & step_ptr = *node.getStep();

    if (step_ptr.getKeys().empty())
    {
        return PredicateConst::TRUE_VALUE;
    }

    ASTPtr underlying_predicate = process(node, context);

    return pullExpressionThroughSymbols(underlying_predicate, step_ptr.getKeys(), context);
}

ASTPtr EffectivePredicateVisitor::visitJoinNode(JoinNode &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}

ASTPtr EffectivePredicateVisitor::visitExchangeNode(ExchangeNode &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}

ASTPtr EffectivePredicateVisitor::visitWindowNode(WindowNode & node, ContextMutablePtr & context)
{
    return process(node, context);
}

ASTPtr EffectivePredicateVisitor::visitMergeSortingNode(MergeSortingNode & node, ContextMutablePtr & context)
{
    return process(node, context);
}

ASTPtr EffectivePredicateVisitor::visitUnionNode(UnionNode &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}

ASTPtr EffectivePredicateVisitor::visitTableScanNode(TableScanNode &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}

ASTPtr EffectivePredicateVisitor::visitDistinctNode(DistinctNode & node, ContextMutablePtr & context)
{
    return process(node, context);
}

ASTPtr EffectivePredicateVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode & node, ContextMutablePtr & context)
{
    return process(node, context);
}

ASTPtr EffectivePredicateVisitor::process(PlanNodeBase & node, ContextMutablePtr & context)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, context);
}

ASTPtr
EffectivePredicateVisitor::pullExpressionThroughSymbols(ASTPtr & expression, std::vector<String> symbols, ContextMutablePtr & context)
{
    EqualityInference equality_inference = EqualityInference::newInstance(expression, context);

    std::vector<ConstASTPtr> effective_conjuncts;
    std::set<String> scope(symbols.begin(), symbols.end());

    for (auto & conjunct : EqualityInference::nonInferrableConjuncts(expression, context))
    {
        if (ExpressionDeterminism::isDeterministic(conjunct, context))
        {
            ASTPtr rewritten = equality_inference.rewrite(conjunct, scope);
            if (rewritten != nullptr)
            {
                effective_conjuncts.emplace_back(rewritten);
            }
        }
    }
    std::vector<ConstASTPtr> equalities = equality_inference.partitionedBy(scope).getScopeEqualities();
    effective_conjuncts.insert(effective_conjuncts.end(), equalities.begin(), equalities.end());

    return PredicateUtils::combineConjuncts(effective_conjuncts);
}

ASTPtr EffectivePredicateVisitor::visitCTERefNode(CTERefNode &, ContextMutablePtr &)
{
    return PredicateConst::TRUE_VALUE;
}
}
