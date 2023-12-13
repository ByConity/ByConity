#include <Optimizer/Rewriter/AddRuntimeFilters.h>

#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Rewriter/PredicatePushdown.h>
#include <Optimizer/Rewriter/UnifyNullableType.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <QueryPlan/GraphvizPrinter.h>

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

    if (!context->getSettingsRef().enable_runtime_filter_pipeline_poll)
    {
        rewrite = AddRuntimeFilters::AddExchange::rewrite(rewrite, context, plan.getCTEInfo());
        plan.update(rewrite);

        print_graphviz("-AddExchange");
    }
}

PlanNodePtr AddRuntimeFilters::AddRuntimeFilterRewriter::visitJoinNode(JoinNode & node, Void & c)
{
    auto & join = *node.getStep();

    // todo left anti
    if (join.getKind() != ASTTableJoin::Kind::Inner && join.getKind() != ASTTableJoin::Kind::Right
          && (join.getStrictness() != ASTTableJoin::Strictness::Semi))
        return visitPlanNode(node, c);

    if (!join.getRuntimeFilterBuilders().empty())
        return visitPlanNode(node, c);

    PlanNodePtr left = VisitorUtil::accept(node.getChildren()[0], *this, c);
    PlanNodePtr right = VisitorUtil::accept(node.getChildren()[1], *this, c);

    std::unordered_map<std::string, DataTypePtr> right_name_to_types;
    for (const auto & item : join.getInputStreams()[1].header)
        right_name_to_types.emplace(item.name, item.type);

    std::vector<ConstASTPtr> probes;
    LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders;

    for (auto left_key = join.getLeftKeys().begin(), right_key = join.getRightKeys().begin();
         left_key != join.getLeftKeys().end(); left_key++, right_key++)
    {
        if (runtime_filter_builders.contains(*right_key))
            continue;
        auto id = context->nextNodeId(); // generate unique id
        probes.emplace_back(RuntimeFilterUtils::createRuntimeFilterExpression(id, *left_key));
        runtime_filter_builders.emplace(*right_key, RuntimeFilterBuildInfos{id, RuntimeFilterDistribution::Distributed});
    }

    if (runtime_filter_builders.empty())
        return visitPlanNode(node, c);

    auto new_join_step = std::make_shared<JoinStep>(
        join.getInputStreams(),
        join.getOutputStream(),
        join.getKind(),
        join.getStrictness(),
        join.getMaxStreams(),
        join.getKeepLeftReadInOrder(),
        join.getLeftKeys(),
        join.getRightKeys(),
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

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        new_join_step,
        PlanNodes{
            PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                std::make_shared<FilterStep>(node.getChildren()[0]->getStep()->getOutputStream(), PredicateUtils::combineConjuncts(probes)),
                PlanNodes{left}),
            right},
        node.getStatistics());
}

AddRuntimeFilters::AddRuntimeFilterRewriter::AddRuntimeFilterRewriter(ContextMutablePtr context_, CTEInfo & cte_info_)
    : SimplePlanRewriter(context_, cte_info_)
{
}

void AddRuntimeFilters::AddRuntimeFilterRewriter::rewrite(QueryPlan & plan)
{
    Void c;
    auto result = VisitorUtil::accept(plan.getPlanNode(), *this, c);
    plan.update(result);
}


RuntimeFilterContext AddRuntimeFilters::RuntimeFilterInfoExtractor::extract(QueryPlan & plan, ContextMutablePtr & context)
{
    RuntimeFilterInfoExtractor visitor{context, plan.getCTEInfo()};
    std::unordered_set<RuntimeFilterId> no_exchange;
    VisitorUtil::accept(plan.getPlanNode(), visitor, no_exchange);
    return RuntimeFilterContext{
        visitor.runtime_filter_build_statistics,
        visitor.runtime_filter_probe_cost,
        visitor.runtime_filter_build_cost,
        visitor.merged_runtime_filters,
        visitor.runtime_filter_distribution};
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    size_t total_rows = 0;
    for (auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, no_exchange);
        total_rows += result.scan_rows;
    }
    return RuntimeFilterWithScanRows{std::unordered_map<std::string, RuntimeFilterId>{}, total_rows};
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitExchangeNode(ExchangeNode & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    auto exchange_step = node.getStep();
    if (exchange_step->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION
        || exchange_step->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
        return VisitorUtil::accept(node.getChildren()[0], *this, no_exchange);
    else
    {
        std::unordered_set<RuntimeFilterId> empty{};
        return VisitorUtil::accept(node.getChildren()[0], *this, empty);
    }
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitProjectionNode(ProjectionNode & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, no_exchange);

    auto project_step = node.getStep();
    std::unordered_map<std::string, RuntimeFilterId> children_runtime_filters;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (assignment.second->getType() == ASTType::ASTIdentifier && result.inherited_runtime_filters.contains(assignment.first))
        {
            auto identifier = assignment.second->as<ASTIdentifier &>();
            children_runtime_filters.emplace(identifier.name(), result.inherited_runtime_filters.at(assignment.first));
        }
    }
    return RuntimeFilterWithScanRows{children_runtime_filters, result.scan_rows};
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    auto join_step = node.getStep();

    std::unordered_set<RuntimeFilterId> left_runtime_filter_without_exchange{no_exchange.begin(), no_exchange.end()};
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
        left_runtime_filter_without_exchange.insert(runtime_filter.second.id);

    auto left_result = VisitorUtil::accept(node.getChildren()[0], *this, left_runtime_filter_without_exchange);
    auto right_result = VisitorUtil::accept(node.getChildren()[1], *this, no_exchange);

    auto left_stats = CardinalityEstimator::estimate(*node.getChildren()[0], cte_info, context);
    auto right_stats = CardinalityEstimator::estimate(*node.getChildren()[1], cte_info, context);
    auto stats = CardinalityEstimator::estimate(node, cte_info, context);
    if (!stats || !left_stats || !right_stats)
        return RuntimeFilterWithScanRows{{}, left_result.scan_rows + right_result.scan_rows};

    // merge builder with the same effect
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
    {
        const auto & name = runtime_filter.first;
        const auto & id = runtime_filter.second.id;

        // register statistics
        runtime_filter_build_statistics.emplace(
            id, std::make_pair(right_stats.value()->getRowCount(), right_stats.value()->getSymbolStatistics(name)));
        runtime_filter_build_cost.emplace(id, right_result.scan_rows);

        // merge duplicate runtime Filter build side.
        if (right_result.inherited_runtime_filters.contains(name))
            merged_runtime_filters.emplace(id, right_result.inherited_runtime_filters.at(name));
        else
        {
            right_result.inherited_runtime_filters[name] = id;
            merged_runtime_filters.emplace(id, id);
        }
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

    for (auto & filter : left_result.inherited_runtime_filters)
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
    for (auto & filter : right_result.inherited_runtime_filters)
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

    return RuntimeFilterWithScanRows{children_runtime_filters, left_result.scan_rows + right_result.scan_rows};
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitTableScanNode(TableScanNode & node, std::unordered_set<RuntimeFilterId> &)
{
    auto stats = CardinalityEstimator::estimate(node, cte_info, context);
    if (!stats)
        return RuntimeFilterWithScanRows{{}, 0L};
    return RuntimeFilterWithScanRows{{}, stats.value()->getRowCount()};
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    auto cte = node.getStep();
    return cte_helper.accept(cte->getId(), *this, no_exchange);
}

RuntimeFilterWithScanRows
AddRuntimeFilters::RuntimeFilterInfoExtractor::visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & no_exchange)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, no_exchange);

    auto filter_step = node.getStep();
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    auto predicates = std::move(filters.second);
    for (auto & runtime_filter : filters.first)
    {
        auto id = RuntimeFilterUtils::extractId(runtime_filter);
        runtime_filter_probe_cost.emplace(id, result.scan_rows);

        if (context->getSettingsRef().enable_local_runtime_filter && no_exchange.contains(id))
            runtime_filter_distribution[id].emplace(RuntimeFilterDistribution::Local);
        else
            runtime_filter_distribution[id].emplace(RuntimeFilterDistribution::Distributed);
    }
    return RuntimeFilterWithScanRows{{}, result.scan_rows};
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::rewrite(const PlanNodePtr & plan)
{
    std::unordered_set<RuntimeFilterId> allowed_runtime_filters;
    return VisitorUtil::accept(plan, *this, allowed_runtime_filters);
}

AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::RemoveUnusedRuntimeFilterProbRewriter(
    ContextMutablePtr context_, CTEInfo & cte_info_, const RuntimeFilterContext & runtime_filter_context_)
    : context(context_), cte_helper(cte_info_), runtime_filter_context(runtime_filter_context_)
{
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitPlanNode(
    PlanNodeBase & plan, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    PlanNodes children;
    for (auto & child : plan.getChildren())
    {
        auto result = VisitorUtil::accept(*child, *this, allowed_runtime_filters);
        children.emplace_back(result);
    }
    auto new_step = plan.getStep()->copy(context);
    plan.setStep(new_step);

    plan.replaceChildren(children);
    return plan.shared_from_this();
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

    auto child_stats = CardinalityEstimator::estimate(*node.getChildren()[0], cte_helper.getCTEInfo(), context);
    auto filter_step = node.getStep();

    if (!child_stats || child_stats.value()->getRowCount() < context->getSettingsRef().runtime_filter_min_filter_rows)
    {
        return PlanNodeBase::createPlanNode(
            node.getId(),
            std::make_shared<FilterStep>(
                child->getCurrentDataStream(), removeAllRuntimeFilters(filter_step->getFilter()), filter_step->removesFilterColumn()),
            PlanNodes{child},
            node.getStatistics());
    }

    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    auto predicates = std::move(filters.second);
    for (auto & runtime_filter : filters.first)
    {
        auto description = RuntimeFilterUtils::extractDescription(runtime_filter).value();
        auto filter_id = description.id;

        // 0. valid
        if (!allowed_runtime_filters.contains(filter_id))
            continue;
        if (!runtime_filter_context.merged_runtime_filters.contains(filter_id))
            continue;

        // update build id
        auto build_id = runtime_filter_context.merged_runtime_filters.at(filter_id);
        description.id = build_id;

        // 1. filter factor
        double filter_factor = 1
            - RuntimeFilterUtils::estimateSelectivity(
                                   description,
                                   runtime_filter_context.runtime_filter_build_statistics.at(build_id).second,
                                   child_stats.value(),
                                   *filter_step,
                                   context);
        if (filter_factor < context->getSettingsRef().runtime_filter_min_filter_factor)
            continue;

        // 2. cost based on complexity
        if (context->getSettingsRef().enable_runtime_filter_cost)
        {
            auto build_cost = runtime_filter_context.runtime_filter_build_cost.at(build_id);
            auto probe_cost = runtime_filter_context.runtime_filter_probe_cost.at(build_id);
            if (probe_cost <= build_cost)
                continue;
        }

        description.filter_factor = filter_factor;
        predicates.emplace_back(RuntimeFilterUtils::createRuntimeFilterExpression(description));
        effective_runtime_filters[build_id].emplace(filter_id);
    }

    return PlanNodeBase::createPlanNode(
        node.getId(),
        std::make_shared<FilterStep>(
            child->getCurrentDataStream(), PredicateUtils::combineConjuncts(predicates), filter_step->removesFilterColumn()),
        PlanNodes{child},
        node.getStatistics());
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitJoinNode(
    JoinNode & plan, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    const auto & join_step = plan.getStep();

    auto right = VisitorUtil::accept(plan.getChildren()[1], *this, allowed_runtime_filters);

    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
        allowed_runtime_filters.emplace(runtime_filter.second.id);
    auto left = VisitorUtil::accept(plan.getChildren()[0], *this, allowed_runtime_filters);

    // remove them runtime Filters in join filter, which are not supported.
    auto filters = removeAllRuntimeFilters(join_step->getFilter());

    return PlanNodeBase::createPlanNode(
        plan.getId(),
        std::make_shared<JoinStep>(
            DataStreams{left->getCurrentDataStream(), right->getCurrentDataStream()},
            join_step->getOutputStream(),
            join_step->getKind(),
            join_step->getStrictness(),
            join_step->getMaxStreams(),
            join_step->getKeepLeftReadInOrder(),
            join_step->getLeftKeys(),
            join_step->getRightKeys(),
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
        plan.getStatistics());
}


PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter::visitCTERefNode(
    CTERefNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters)
{
    auto cte = node.getStep();
    cte_helper.accept(cte->getId(), *this, allowed_runtime_filters);
    return node.shared_from_this();
}

AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::RemoveUnusedRuntimeFilterBuildRewriter(
    ContextMutablePtr & context_,
    CTEInfo & cte_info,
    const std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterId>> & effective_runtime_filters_,
    const RuntimeFilterContext & runtime_filter_context_)
    : context(context_)
    , cte_helper(cte_info)
    , effective_runtime_filters(effective_runtime_filters_)
    , runtime_filter_context(runtime_filter_context_)
{
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::rewrite(PlanNodePtr & plan)
{
    Void c;
    return VisitorUtil::accept(plan, *this, c);
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::visitPlanNode(PlanNodeBase & plan, Void & c)
{
    PlanNodes children;
    for (auto & child : plan.getChildren())
    {
        auto result = VisitorUtil::accept(*child, *this, c);
        children.emplace_back(result);
    }

    auto new_step = plan.getStep()->copy(context);
    plan.setStep(new_step);
    plan.replaceChildren(children);
    return plan.shared_from_this();
}

PlanNodePtr AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter::visitJoinNode(JoinNode & node, Void & c)
{
    auto join_step = node.getStep();

    std::unordered_map<String, RuntimeFilterBuildInfos> runtime_filter_builders;
    for (const auto & runtime_filter : join_step->getRuntimeFilterBuilders())
        if (effective_runtime_filters.contains(runtime_filter.second.id))
        {
            // any runtime filter probe is distributed, build is distributed.
            RuntimeFilterDistribution distribution = RuntimeFilterDistribution::Local;
            if (runtime_filter_context.runtime_filter_distribution.at(runtime_filter.second.id)
                    .contains(RuntimeFilterDistribution::Distributed))
                distribution = RuntimeFilterDistribution::Distributed;

            for (const auto & filter_id : effective_runtime_filters.at(runtime_filter.second.id))
                if (filter_id != runtime_filter.second.id)
                    distribution = RuntimeFilterDistribution::Distributed;

            runtime_filter_builders.emplace(runtime_filter.first, RuntimeFilterBuildInfos{runtime_filter.second.id, distribution});
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
    auto cte = node.getStep();
    cte_helper.acceptAndUpdate(cte->getId(), *this, c);
    return node.shared_from_this();
}


PlanNodePtr AddRuntimeFilters::AddExchange::rewrite(const PlanNodePtr & node, ContextMutablePtr context_, CTEInfo & cte_info_)
{
    AddExchange rewriter{context_, cte_info_};
    bool has_exchange = false;
    return VisitorUtil::accept(node, rewriter, has_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitPlanNode(PlanNodeBase & node, bool &)
{
    bool has_exchange = false;
    return SimplePlanRewriter::visitPlanNode(node, has_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitExchangeNode(ExchangeNode & node, bool &)
{
    bool has_exchange = true;
    return SimplePlanRewriter::visitPlanNode(node, has_exchange);
}

PlanNodePtr AddRuntimeFilters::AddExchange::visitFilterNode(FilterNode & node, bool & has_exchange)
{
    bool has_exchange_visit_child = has_exchange;
    auto result = SimplePlanRewriter::visitPlanNode(node, has_exchange_visit_child);
    if (has_exchange)
        return result;

    /// enforce local exchange
    auto filter_step = node.getStep();
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter());
    if (filters.first.empty())
        return result;

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_unique<ExchangeStep>(
            DataStreams{result->getCurrentDataStream()},
            ExchangeMode::LOCAL_NO_NEED_REPARTITION,
            Partitioning{Partitioning::Handle::FIXED_ARBITRARY},
            context->getSettingsRef().enable_shuffle_with_order),
        PlanNodes{result});
}
}
