#include <Optimizer/Rewriter/AddRuntimeFilters.h>

#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/PropertyDeriver.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/UnifyNullableType.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/PlanNode.h>
#include <Poco/StringTokenizer.h>
#include <common/logger_useful.h>

#include <algorithm>
#include <memory>

namespace DB
{
void AddRuntimeFilters::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    Stopwatch rule_watch;
    auto print_graphviz = [&](const String & hint) {
        rule_watch.stop();
        GraphvizPrinter::printLogicalPlan(
            plan, context, std::to_string(context->getRuleId()) + hint + std::to_string(rule_watch.elapsedMillisecondsAsDouble()) + "ms");
        rule_watch.restart();
    };

    AddRuntimeFilterRewriter add_runtime_filter_rewriter(context, plan.getCTEInfo());
    add_runtime_filter_rewriter.rewrite(plan);

    print_graphviz("-AddRuntimeFilterRewriter");

    // 2.must run UnifyNullableType before PredicatePushdown
    UnifyNullableType unify_nullable_type;
    unify_nullable_type.rewritePlan(plan, context);

    // 3.push down predicate with dynamic filter enabled
    PredicatePushdown predicate_push_down;
    predicate_push_down.rewritePlan(plan, context);

    // 4. extract all dynamic filter builders from projection,
    // then try to merge dynamic filters with the same effect
    auto runtime_filter_context = RuntimeFilterInfoExtractor::extract(plan, context);

    print_graphviz("-RuntimeFilterInfoExtractorResult");

    // extract and filter effective dynamic filter execute predicates with statistics.
    RemoveUnusedRuntimeFilterProbRewriter runtime_filter_prob_rewriter{context, plan.getCTEInfo(), runtime_filter_context};
    auto rewrite = runtime_filter_prob_rewriter.rewrite(plan.getPlanNode());

    print_graphviz("-RemoveUnusedRuntimeFilterProbRewriter");

    RemoveUnusedRuntimeFilterBuildRewriter runtime_filter_build_rewriter{
        context, plan.getCTEInfo(), runtime_filter_prob_rewriter.getEffectiveRuntimeFilters(), runtime_filter_context};
    rewrite = runtime_filter_build_rewriter.rewrite(rewrite);

    print_graphviz("-RemoveUnusedRuntimeFilterBuildRewriter");

    if (!context->getSettingsRef().enable_runtime_filter_pipeline_poll || !plan.getCTEInfo().empty())
    {
        rewrite = AddRuntimeFilters::AddExchange::rewrite(rewrite, context, plan.getCTEInfo());
        print_graphviz("-AddExchange");
    }
    plan.update(rewrite);
}

PlanPropEquivalences AddRuntimeFilters::AddRuntimeFilterRewriter::visitPlanNode(PlanNodeBase & node, Void & c)
{
    PlanNodes children;
    std::vector<SymbolEquivalencesPtr> children_equivalences;
    PropertySet input_properties;
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, c);
        children.emplace_back(result.plan);
        children_equivalences.emplace_back(result.equivalences);
        input_properties.emplace_back(result.property);
    }
    return replaceChildren(node, std::move(children), std::move(children_equivalences), std::move(input_properties));
}

PlanPropEquivalences AddRuntimeFilters::AddRuntimeFilterRewriter::replaceChildren(
    PlanNodeBase & node,
    PlanNodes children,
    std::vector<SymbolEquivalencesPtr> children_equivalences,
    PropertySet input_properties)
{
    node.replaceChildren(children);
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), input_properties, any_prop, context);
    auto equivalences = SymbolEquivalencesDeriver::deriveEquivalences(node.getStep(), children_equivalences);
    prop = prop.normalize(*equivalences);
    return PlanPropEquivalences{node.shared_from_this(), prop, equivalences};
}

PlanPropEquivalences AddRuntimeFilters::AddRuntimeFilterRewriter::visitCTERefNode(CTERefNode & node, Void & c)
{
    const auto * step = node.getStep().get();
    auto result = cte_helper.accept(step->getId(), *this, c);
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
    auto equivalences = SymbolEquivalencesDeriver::deriveEquivalences(node.getStep(), {result.equivalences});
    prop = prop.normalize(*equivalences);
    return {node.shared_from_this(), prop, equivalences};
}

static bool isFixedHashShuffleOrBucketTableShuffle(const Property & property)
{
    return property.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_HASH
        || property.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE;
}

PlanPropEquivalences AddRuntimeFilters::AddRuntimeFilterRewriter::visitJoinNode(JoinNode & node, Void & c)
{
    const auto & join = *node.getStep();

    // todo left anti
    if (join.getKind() != ASTTableJoin::Kind::Inner && join.getKind() != ASTTableJoin::Kind::Right
        && join.getStrictness() != ASTTableJoin::Strictness::Semi)
        return visitPlanNode(node, c);

    if (!join.getRuntimeFilterBuilders().empty())
        return visitPlanNode(node, c);

    auto left = VisitorUtil::accept(node.getChildren()[0], *this, c);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, c);

    auto left_stats = CardinalityEstimator::estimate(*left.plan, cte_info, context);
    auto right_stats = CardinalityEstimator::estimate(*right.plan, cte_info, context);
    auto stats = CardinalityEstimator::estimate(node, cte_info, context);
    if (!stats || !left_stats || !right_stats)
        return replaceChildren(node, {left.plan, right.plan}, {left.equivalences, right.equivalences}, {left.property, right.property});


    bool is_broadcast = join.getDistributionType() == DistributionType::BROADCAST;

    std::vector<ConstASTPtr> probes;
    LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders;

    for (auto left_key = join.getLeftKeys().begin(), right_key = join.getRightKeys().begin(); left_key != join.getLeftKeys().end();
         left_key++, right_key++)
    {
        if (runtime_filter_builders.contains(*right_key))
            continue;

        auto left_symbol_stats = left_stats->get()->getSymbolStatistics(*left_key);
        auto right_symbol_stats = right_stats->get()->getSymbolStatistics(*right_key);
        if (left_symbol_stats->isUnknown() || right_symbol_stats->isUnknown())
        {
            continue;
        }

        Names partition_columns;
        if (!is_broadcast && isFixedHashShuffleOrBucketTableShuffle(left.property))
        {
            partition_columns = left.property.getNodePartitioning().getColumns();
            bool all_contains = std::all_of(
                partition_columns.begin(), partition_columns.end(), [&](const auto & column) {
                    return left.plan->getStep()->getOutputStream().header.has(column);
                });
            if (!all_contains)
            {
                LOG_WARNING(
                    logger,
                    "partition columns not found in AddRuntimeFilteres, required: {}, left output: {}",
                    fmt::join(partition_columns, ", "),
                    fmt::join(left.plan->getOutputNames(), ", "));
                break;
            }
        }

        double selectivity;
        if (join.getLeftKeys().size() == 1 && PredicateUtils::isTruePredicate(join.getFilter()))
        {
            // join estimator always more accurate
            selectivity = static_cast<double>((*stats)->getRowCount()) / (*left_stats)->getRowCount();
        }
        else
        {
            // simple use ndv as filter factor
            selectivity = RuntimeFilterUtils::estimateSelectivity(
                std::make_shared<ASTIdentifier>(*left_key), right_symbol_stats, *left_stats, left.plan->getOutputNamesAndTypes(), context);
        }
        double filter_factor = 1 - selectivity;

        auto filter_id = nextId(); // generate unique id
        probes.emplace_back(RuntimeFilterUtils::createRuntimeFilterExpression(filter_id, *left_key, partition_columns, filter_factor));

        RuntimeFilterDistribution distribution = RuntimeFilterDistribution::UNKNOWN;
        runtime_filter_builders.emplace(*right_key, RuntimeFilterBuildInfos{filter_id, distribution});
    }

    if (runtime_filter_builders.empty())
        return replaceChildren(node, {left.plan, right.plan}, {left.equivalences, right.equivalences}, {left.property, right.property});

    auto new_join_step = std::make_shared<JoinStep>(
        join.getInputStreams(),
        join.getOutputStream(),
        join.getKind(),
        join.getStrictness(),
        join.getMaxStreams(),
        join.getKeepLeftReadInOrder(),
        join.getLeftKeys(),
        join.getRightKeys(),
        join.getKeyIdsNullSafe(),
        join.getFilter(),
        join.isHasUsing(),
        join.getRequireRightKeys(),
        join.getAsofInequality(),
        join.getDistributionType(),
        join.getJoinAlgorithm(),
        join.isMagic(),
        join.isOrdered(),
        join.isSimpleReordered(),
        runtime_filter_builders,
        join.getHints());

    PropertySet input_properties{left.property, right.property};
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), input_properties, any_prop, context);

    std::vector<SymbolEquivalencesPtr> children_equivalences{left.equivalences, right.equivalences};
    auto equivalences = SymbolEquivalencesDeriver::deriveEquivalences(node.getStep(), children_equivalences);
    prop = prop.normalize(*equivalences);

    return PlanPropEquivalences{
        PlanNodeBase::createPlanNode(
            context->nextNodeId(),
            new_join_step,
            PlanNodes{
                PlanNodeBase::createPlanNode(
                    context->nextNodeId(),
                    std::make_shared<FilterStep>(
                        node.getChildren()[0]->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(probes)),
                    PlanNodes{left.plan}),
                right.plan},
            node.getStatistics()),
        prop,
        equivalences};
}

void AddRuntimeFilters::AddRuntimeFilterRewriter::rewrite(QueryPlan & plan)
{
    Void c;
    auto result = VisitorUtil::accept(plan.getPlanNode(), *this, c);
    plan.update(result.plan);
}


RuntimeFilterContext AddRuntimeFilters::RuntimeFilterInfoExtractor::extract(QueryPlan & plan, ContextMutablePtr & context)
{
    RuntimeFilterInfoExtractor visitor{context, plan.getCTEInfo()};
    std::unordered_set<RuntimeFilterId> no_exchange;
    VisitorUtil::accept(plan.getPlanNode(), visitor, no_exchange);
    return std::move(visitor.runtime_filter_context);
}

InheritedRuntimeFilters AddRuntimeFilters::RuntimeFilterInfoExtractor::visitPlanNode(
    PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter)
{
    for (auto & child : node.getChildren())
        VisitorUtil::accept(child, *this, local_runtime_filter);
    return {};
}

InheritedRuntimeFilters AddRuntimeFilters::RuntimeFilterInfoExtractor::visitExchangeNode(
    ExchangeNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter)
{
    const auto * exchange_step = dynamic_cast<const ExchangeStep *>(node.getStep().get());
    if (exchange_step->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION
        || exchange_step->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
        return VisitorUtil::accept(node.getChildren()[0], *this, local_runtime_filter);
    else
    {
        std::unordered_set<RuntimeFilterId> empty{};
        return VisitorUtil::accept(node.getChildren()[0], *this, empty);
    }
}

InheritedRuntimeFilters AddRuntimeFilters::RuntimeFilterInfoExtractor::visitProjectionNode(
    ProjectionNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, local_runtime_filter);

    const auto * project_step = dynamic_cast<const ProjectionStep *>(node.getStep().get());
    std::unordered_map<std::string, RuntimeFilterId> children_runtime_filters;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (assignment.second->getType() == ASTType::ASTIdentifier && result.contains(assignment.first))
            children_runtime_filters.emplace(assignment.second->as<ASTIdentifier &>().name(), result.at(assignment.first));
    }
    return children_runtime_filters;
}

InheritedRuntimeFilters
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter)
{
    const auto * join_step = dynamic_cast<const JoinStep *>(node.getStep().get());

    std::unordered_set<RuntimeFilterId> left_local_runtime_filter{local_runtime_filter.begin(), local_runtime_filter.end()};
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
        left_local_runtime_filter.insert(runtime_filter.second.id);

    auto left_result = VisitorUtil::accept(node.getChildren()[0], *this, left_local_runtime_filter);
    auto right_result = VisitorUtil::accept(node.getChildren()[1], *this, local_runtime_filter);

    auto left_stats = CardinalityEstimator::estimate(*node.getChildren()[0], cte_info, context);
    auto right_stats = CardinalityEstimator::estimate(*node.getChildren()[1], cte_info, context);
    auto stats = CardinalityEstimator::estimate(node, cte_info, context);
    if (!stats || !left_stats || !right_stats)
        return {};

    // try merge builder with the same effect
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
    {
        const auto & name = runtime_filter.first;
        const auto & id = runtime_filter.second.id;

        // register statistics
        runtime_filter_context.runtime_filter_build_statistics.emplace(id, right_stats.value()->getSymbolStatistics(name));

        // merge duplicate runtime Filter build side.
        if (right_result.contains(name))
            runtime_filter_context.merged_runtime_filters.emplace(id, right_result.at(name));
        else
            right_result[name] = id;
    }

    // find and return reusable runtime filter builder to parent node.
    std::unordered_map<std::string, RuntimeFilterId> children_runtime_filters;
    NameSet outputs;
    for (const auto & name_and_type : join_step->getOutputStream().header)
        outputs.emplace(name_and_type.name);

    std::unordered_map<std::string, std::string> left_to_right;
    std::unordered_map<std::string, std::string> right_to_left;
    for (auto left = join_step->getLeftKeys().begin(), right = join_step->getRightKeys().begin(); left != join_step->getLeftKeys().end();
         ++left, ++right)
    {
        left_to_right[*left] = *right;
        right_to_left[*right] = *left;
    }

    for (auto & filter : left_result)
    {
        std::string output;
        if (outputs.contains(filter.first))
            output = filter.first;
        else if (left_to_right.contains(filter.first) && outputs.contains(left_to_right.at(filter.first)))
            output = left_to_right.at(filter.first);
        else
            continue;

        if (!left_stats.value()->getSymbolStatistics(filter.first)->isUnknown() && !stats.value()->getSymbolStatistics(output)->isUnknown()
            && left_stats.value()->getSymbolStatistics(filter.first)->getNdv() <= stats.value()->getSymbolStatistics(output)->getNdv())
            children_runtime_filters.emplace(output, filter.second);
    }
    for (auto & filter : right_result)
    {
        std::string output;
        if (outputs.contains(filter.first))
            output = filter.first;
        else if (right_to_left.contains(filter.first) && outputs.contains(right_to_left.at(filter.first)))
            output = right_to_left.at(filter.first);
        else
            continue;

        if (!right_stats.value()->getSymbolStatistics(filter.first)->isUnknown() && !stats.value()->getSymbolStatistics(output)->isUnknown()
            && right_stats.value()->getSymbolStatistics(filter.first)->getNdv() <= stats.value()->getSymbolStatistics(output)->getNdv())
            children_runtime_filters.emplace(output, filter.second);
    }

    return children_runtime_filters;
}


InheritedRuntimeFilters
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> &)
{
    const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
    std::unordered_set<RuntimeFilterId> empty;
    cte_helper.accept(cte->getId(), *this, empty);
    return {};
}

InheritedRuntimeFilters AddRuntimeFilters::RuntimeFilterInfoExtractor::visitFilterNode(
    FilterNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, local_runtime_filter);

    const auto * filter_step = dynamic_cast<const FilterStep *>(node.getStep().get());
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    auto predicates = std::move(filters.second);
    for (auto & runtime_filter : filters.first)
    {
        auto id = RuntimeFilterUtils::extractId(runtime_filter);
        if (!local_runtime_filter.contains(id))
            runtime_filter_context.distributed_runtime_filters.emplace(id);
    }
    return {};
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::rewrite(const PlanNodePtr & plan)
{
    std::unordered_set<RuntimeFilterId> allowed_runtime_filters;
    return VisitorUtil::accept(plan, *this, allowed_runtime_filters);
}

AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::RemoveUnusedRuntimeFilterProbRewriter(
    ContextMutablePtr context_, CTEInfo & cte_info_, RuntimeFilterContext & runtime_filter_context_)
    : context(context_), cte_helper(cte_info_), runtime_filter_context(runtime_filter_context_)
{
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitPlanNode(
    PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    PlanNodes children;
    for (auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(*child, *this, allowed_runtime_filters);
        children.emplace_back(result);
    }
    node.replaceChildren(children);
    return node.shared_from_this();
}

static ConstASTPtr removeAllRuntimeFilters(const ConstASTPtr & expr)
{
    std::vector<ConstASTPtr> ret;
    auto filters = PredicateUtils::extractConjuncts(expr);
    for (auto & filter : filters)
        if (!RuntimeFilterUtils::isInternalRuntimeFilter(filter))
            ret.emplace_back(filter);

    if (ret.size() == filters.size())
        return expr;

    return PredicateUtils::combineConjuncts(ret);
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitFilterNode(
    FilterNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    auto child = VisitorUtil::accept(*node.getChildren()[0], *this, allowed_runtime_filters);

    String black_list = context->getSettingsRef().runtime_filter_black_list;

    Poco::StringTokenizer st(black_list, ",");
    for (const auto & tocken : st)
    {
        allowed_runtime_filters.erase(std::stoi(tocken));
    }

    auto child_stats = CardinalityEstimator::estimate(*node.getChildren()[0], cte_helper.getCTEInfo(), context);
    const auto & filter_step = node.getStep();

    if (!child_stats || child_stats.value()->getRowCount() < context->getSettingsRef().runtime_filter_min_filter_rows)
    {
        return PlanNodeBase::createPlanNode(
            node.getId(),
            std::make_shared<FilterStep>(
                child->getCurrentDataStream(), removeAllRuntimeFilters(filter_step->getFilter()), filter_step->removesFilterColumn()),
            PlanNodes{child},
            node.getStatistics());
    }

    bool is_table_scan_filter = node.getChildren()[0]->getType() == IQueryPlanStep::Type::TableScan;

    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    auto predicates = std::move(filters.second);
    for (auto & runtime_filter : filters.first)
    {
        auto description = RuntimeFilterUtils::extractDescription(runtime_filter).value();
        // 0. check
        if (!allowed_runtime_filters.contains(description.id))
            continue;

        // 1. merge runtime filter build if distributed
        if (runtime_filter_context.merged_runtime_filters.contains(description.id))
        {
            auto build_id = runtime_filter_context.merged_runtime_filters.at(description.id);
            if (runtime_filter_context.distributed_runtime_filters.contains(description.id)
                && runtime_filter_context.distributed_runtime_filters.contains(build_id))
                description.id = build_id;
        }

        // 2. update filter factor
        double selectivity = RuntimeFilterUtils::estimateSelectivity(
            description.expr,
            runtime_filter_context.runtime_filter_build_statistics.at(description.id),
            *child_stats,
            child->getOutputNamesAndTypes(),
            context);
        description.filter_factor = std::max(description.filter_factor, 1 - selectivity);

        // 3. filter by filter factor
        double min_filter_factor = is_table_scan_filter ? context->getSettingsRef().runtime_filter_min_filter_factor
                                                        : context->getSettingsRef().runtime_filter_min_filter_factor_for_non_table_scan;
        if (description.filter_factor < min_filter_factor)
            continue;

        predicates.emplace_back(RuntimeFilterUtils::createRuntimeFilterExpression(description));
        effective_runtime_filters.emplace(description.id);
    }

    return PlanNodeBase::createPlanNode(
        node.getId(),
        std::make_shared<FilterStep>(
            child->getCurrentDataStream(), PredicateUtils::combineConjuncts(predicates), filter_step->removesFilterColumn()),
        PlanNodes{child},
        node.getStatistics());
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitJoinNode(
    JoinNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    const auto * join_step = dynamic_cast<const JoinStep *>(node.getStep().get());

    std::unordered_set<RuntimeFilterId> left_allowed_runtime_filters{allowed_runtime_filters.begin(), allowed_runtime_filters.end()};
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
        left_allowed_runtime_filters.emplace(runtime_filter.second.id);
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, left_allowed_runtime_filters);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, allowed_runtime_filters);

    // remove them runtime Filters in join filter, which are not supported.
    auto filters = removeAllRuntimeFilters(join_step->getFilter());

    return PlanNodeBase::createPlanNode(
        node.getId(),
        std::make_shared<JoinStep>(
            DataStreams{left->getCurrentDataStream(), right->getCurrentDataStream()},
            join_step->getOutputStream(),
            join_step->getKind(),
            join_step->getStrictness(),
            join_step->getMaxStreams(),
            join_step->getKeepLeftReadInOrder(),
            join_step->getLeftKeys(),
            join_step->getRightKeys(),
            join_step->getKeyIdsNullSafe(),
            filters,
            join_step->isHasUsing(),
            join_step->getRequireRightKeys(),
            join_step->getAsofInequality(),
            join_step->getDistributionType(),
            join_step->getJoinAlgorithm(),
            join_step->isMagic(),
            join_step->isOrdered(),
            join_step->isSimpleReordered(),
            join_step->getRuntimeFilterBuilders(),
            join_step->getHints()),
        PlanNodes{left, right},
        node.getStatistics());
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitCTERefNode(
    CTERefNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
    cte_helper.accept(cte->getId(), *this, allowed_runtime_filters);
    return node.shared_from_this();
}

AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::RemoveUnusedRuntimeFilterBuildRewriter(
    ContextMutablePtr & context_,
    CTEInfo & cte_info,
    const std::unordered_set<RuntimeFilterId> & effective_runtime_filters_,
    const RuntimeFilterContext & runtime_filter_context_)
    : context(context_)
    , cte_helper(cte_info)
    , effective_runtime_filters(effective_runtime_filters_)
    , runtime_filter_context(runtime_filter_context_)
{
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::rewrite(PlanNodePtr & node)
{
    Void c;
    return VisitorUtil::accept(node, *this, c);
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::visitPlanNode(PlanNodeBase & node, Void & c)
{
    if (node.getChildren().empty())
        return node.shared_from_this();
    PlanNodes children;
    for (auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(*child, *this, c);
        children.emplace_back(result);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::visitJoinNode(JoinNode & node, Void & c)
{
    const auto * join_step = dynamic_cast<const JoinStep *>(node.getStep().get());

    std::unordered_map<String, RuntimeFilterBuildInfos> runtime_filter_builders;
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
    {
        if (effective_runtime_filters.contains(runtime_filter.second.id))
        {
            // any runtime filter probe is distributed, build is distributed.
            RuntimeFilterDistribution distribution = runtime_filter.second.distribution;
            if (distribution == RuntimeFilterDistribution::UNKNOWN)
                distribution = runtime_filter_context.distributed_runtime_filters.contains(runtime_filter.second.id)
                    ? RuntimeFilterDistribution::DISTRIBUTED
                    : RuntimeFilterDistribution::LOCAL;

            runtime_filter_builders.emplace(runtime_filter.first, RuntimeFilterBuildInfos{runtime_filter.second.id, distribution});
        }
    }

    auto new_join_step = std::make_shared<JoinStep>(
        join_step->getInputStreams(),
        join_step->getOutputStream(),
        join_step->getKind(),
        join_step->getStrictness(),
        join_step->getMaxStreams(),
        join_step->getKeepLeftReadInOrder(),
        join_step->getLeftKeys(),
        join_step->getRightKeys(),
        join_step->getKeyIdsNullSafe(),
        join_step->getFilter(),
        join_step->isHasUsing(),
        join_step->getRequireRightKeys(),
        join_step->getAsofInequality(),
        join_step->getDistributionType(),
        join_step->getJoinAlgorithm(),
        join_step->isMagic(),
        join_step->isOrdered(),
        join_step->isSimpleReordered(),
        LinkedHashMap<String, RuntimeFilterBuildInfos>{runtime_filter_builders.begin(), runtime_filter_builders.end()},
        join_step->getHints());

    node.setStep(new_join_step);
    return visitPlanNode(node, c);
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::visitCTERefNode(CTERefNode & node, Void & c)
{
    const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
    cte_helper.acceptAndUpdate(cte->getId(), *this, c);
    return node.shared_from_this();
}


PlanNodePtr AddRuntimeFilters::AddExchange::rewrite(const PlanNodePtr & node, ContextMutablePtr context_, CTEInfo & cte_info_)
{
    AddExchange rewriter{context_, cte_info_};
    std::unordered_set<RuntimeFilterId> need_exchange;
    return VisitorUtil::accept(node, rewriter, need_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & need_exchange)
{
    return SimplePlanRewriter::visitPlanNode(node, need_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitExchangeNode(ExchangeNode & node, std::unordered_set<RuntimeFilterId> &)
{
    std::unordered_set<RuntimeFilterId> need_exchange;
    return SimplePlanRewriter::visitPlanNode(node, need_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & need_exchange)
{
    const auto & join_step = node.getStep();
    auto right = VisitorUtil::accept(*node.getChildren()[1], *this, need_exchange);
    for (const auto & filter : join_step->getRuntimeFilterBuilders())
        need_exchange.emplace(filter.second.id);
    auto left = VisitorUtil::accept(*node.getChildren()[0], *this, need_exchange);
    node.replaceChildren(PlanNodes{left, right});
    return node.shared_from_this();
}

// fixme: fix buffer step to remove this method
PlanNodePtr AddRuntimeFilters::AddExchange::visitBufferNode(BufferNode & node, std::unordered_set<RuntimeFilterId> & need_exchange)
{
    auto res = SimplePlanRewriter::visitBufferNode(node, need_exchange);
    if (need_exchange.empty())
        return res;

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_unique<ExchangeStep>(
            DataStreams{res->getCurrentDataStream()},
            ExchangeMode::LOCAL_NO_NEED_REPARTITION,
            Partitioning{Partitioning::Handle::FIXED_ARBITRARY},
            context->getSettingsRef().enable_shuffle_with_order),
        PlanNodes{res},
        res->getStatistics());
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> &)
{
    std::unordered_set<RuntimeFilterId> need_exchange;
    return SimplePlanRewriter::visitCTERefNode(node, need_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & need_exchange)
{
    if (need_exchange.empty() || context->getSettingsRef().enable_runtime_filter_pipeline_poll)
        return SimplePlanRewriter::visitPlanNode(node, need_exchange);

    const auto * filter_step = dynamic_cast<const FilterStep *>(node.getStep().get());
    std::vector<RuntimeFilterId> ids = RuntimeFilterUtils::extractRuntimeFilterId(filter_step->getFilter());
    bool is_need_exchage = std::any_of(ids.begin(), ids.end(), [&](const auto & id) { return need_exchange.contains(id); });
    if (!is_need_exchage)
        return SimplePlanRewriter::visitPlanNode(node, need_exchange);

    std::unordered_set<RuntimeFilterId> child_need_exchange;
    auto child = SimplePlanRewriter::visitPlanNode(node, child_need_exchange);
    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_unique<ExchangeStep>(
            DataStreams{child->getCurrentDataStream()},
            ExchangeMode::LOCAL_NO_NEED_REPARTITION,
            Partitioning{Partitioning::Handle::FIXED_ARBITRARY},
            context->getSettingsRef().enable_shuffle_with_order),
        PlanNodes{child},
        node.getStatistics());
}
}
