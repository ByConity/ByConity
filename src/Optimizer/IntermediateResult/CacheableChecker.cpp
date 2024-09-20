#include <Optimizer/IntermediateResult/CacheableChecker.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/ExpandStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/MergingAggregatedStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/TableScanStep.h>

#include <optional>
#include <unordered_set>

namespace DB::CacheableChecker
{
namespace
{
    /**
     * steps allowed in the cached subtree
     * currently supports: TableScan, Filter, Projection, Join, Aggregating, Exchange and MergingAggregated\n
     * all expressions must be deterministic
     */
    class SubtreeStepsChecker : public StepVisitor<bool, ContextPtr>
    {
    public:
        static bool check(const QueryPlanStepPtr & step, ContextPtr context)
        {
            static SubtreeStepsChecker visitor;
            return VisitorUtil::accept(step, visitor, context);
        }

        static bool containsNonDeterministicFunction(ConstASTPtr ast, ContextPtr context)
        {
            if (auto func = dynamic_pointer_cast<const ASTFunction>(ast))
            {
                if (context->isNonDeterministicFunctionOutOfQueryScope(func->name))
                    return true;
            }
            for (const auto & child : ast->children)
            {
                if (containsNonDeterministicFunction(child, context))
                    return true;
            }
            return false;
        }

    protected:
        // by default, a step is not allowed
        bool visitStep(const IQueryPlanStep &, ContextPtr &) override { return false; }

        bool visitAggregatingStep(const AggregatingStep &, ContextPtr &) override { return true; }

        bool visitTableScanStep(const TableScanStep & step, ContextPtr & context) override
        {
            if (step.getQueryInfo().getSelectQuery()->sampleSize() != nullptr
                || step.getQueryInfo().getSelectQuery()->sampleOffset() != nullptr)
                return false;
            if (auto prewhere = step.getPrewhere())
            {
                if (containsNonDeterministicFunction(step.getPrewhere(), context))
                    return false;
            }
            return true;
        }

        bool visitJoinStep(const JoinStep & step, ContextPtr & context) override
        {
            return !containsNonDeterministicFunction(step.getFilter(), context);
        }

        bool visitMergingAggregatedStep(const MergingAggregatedStep &, ContextPtr &) override { return true; }
        bool visitExpandStep(const ExpandStep &, ContextPtr &) override { return true; }

        bool visitExchangeStep(const ExchangeStep & step, ContextPtr &) override
        {
            switch (step.getExchangeMode())
            {
                // only support deterministic exchanges
                case ExchangeMode::BROADCAST:
                    return true;
                case ExchangeMode::REPARTITION:
                    return step.getSchema().getHandle() == Partitioning::Handle::FIXED_HASH;
                default:
                    return false;
            }
        }

        bool visitProjectionStep(const ProjectionStep & step, ContextPtr & context) override
        {
            for (const auto & assigment : step.getAssignments())
                if (containsNonDeterministicFunction(assigment.second, context))
                    return false;
            return true;
        }

        bool visitFilterStep(const FilterStep & step, ContextPtr & context) override
        {
            return !containsNonDeterministicFunction(step.getFilter(), context);
        }
    };

    class SubtreeNodesAndDeterminismChecker : public PlanNodeVisitor<bool, ContextPtr>
    {
    protected:
        bool visitPlanNode(PlanNodeBase & node, ContextPtr & context) override
        {
            bool allowed = SubtreeStepsChecker::check(node.getStep(), context);
            if (!allowed)
                return false;
            for (const auto & child : node.getChildren())
                if (!VisitorUtil::accept(*child, *this, context))
                    return false;
            return true;
        }
    };
}

/**
     * a subtree is valid for cache if
     * 1) it is rooted at partial agg\n
     * 2) (for simplicity, may be relaxed) only {Aggregating, Projection, Filter, TableScan, Join, Exchange, MergingAggregated} nodes exists in the subtree\n
     * 3) its left-most path (excluding root) only contains {Projection, Filter, TableScan, Join} \n
     * 4) its left-bottom step is a TableScan
     */
bool isValidForCache(PlanNodePtr node, ContextPtr context)
{
    // check 1) root
    auto * agg = dynamic_cast<AggregatingStep *>(node->getStep().get());
    if (!agg)
        return false;

    if (agg->isGroupingSet())
        return false;

    bool allow_join = context->getSettingsRef().enable_join_intermediate_result_cache;

    // check 2) left-most path (excluding root) only contains {Projection, Filter, TableScan, Join}
    PlanNodePtr current = node;
    while (!current->getChildren().empty())
    {
        current = current->getChildren().front();
        if (!isAllowedInLeftMostPath(current, allow_join))
            return false;
    }
    // check 3) left-bottom step is a TableScan
    if (current->getType() != IQueryPlanStep::Type::TableScan)
        return false;
    auto * table_scan = dynamic_cast<TableScanStep *>(current->getStep().get());
    if (Poco::toLower(table_scan->getDatabase()) == "system")
        return false;
    auto storage =  table_scan->getStorage();
    if (!storage->supportIntermedicateResultCache())
        return false;

    // check 4) check subtree allowed
    return isSubtreeDeterministic(node, context);
}

// nodes allowed in the left-most path, excluding the top agg
bool isAllowedInLeftMostPath(PlanNodePtr node, bool allow_join)
{
    auto type = node->getStep()->getType();
    switch (type)
    {
        case IQueryPlanStep::Type::Join:
            return allow_join;
        // mark distinct is stateful so the result of chunk can not be cached
        case IQueryPlanStep::Type::MarkDistinct:
            return false;
        // expand is stateless
        case IQueryPlanStep::Type::Expand:
            [[fallthrough]];
        case IQueryPlanStep::Type::Projection:
            [[fallthrough]];
        case IQueryPlanStep::Type::Filter:
            [[fallthrough]];
        case IQueryPlanStep::Type::TableScan:
            return true;
        default:
            return false;
    }
}

bool isSubtreeDeterministic(PlanNodePtr node, ContextPtr context)
{
    SubtreeNodesAndDeterminismChecker checker;
    return VisitorUtil::accept(node, checker, context);
}

RuntimeFilterBuildsAndProbes RuntimeFilterCollector::collect(PlanNodePtr plan, const CTEInfo & cte_info)
{
    RuntimeFilterBuildsAndProbes res;
    std::unordered_set<CTEId> visited_ctes;
    collectImpl(plan, cte_info, res, visited_ctes);
    return res;
}

void RuntimeFilterCollector::collectImpl(PlanNodePtr node, const CTEInfo & cte_info,
                                         RuntimeFilterBuildsAndProbes & res, std::unordered_set<CTEId> & visited_ctes)
{
    if (auto join_step = dynamic_pointer_cast<JoinStep>(node->getStep()))
    {
        for (const auto & [symbol, info] : join_step->getRuntimeFilterBuilders())
        {
            res.builds.emplace(info.id, RuntimeFilterBuildInfo{node, symbol, info});
        }
    }
    else if (auto filter_step = dynamic_pointer_cast<FilterStep>(node->getStep()))
    {
        collectProbesImpl(filter_step->getFilter(), node, res);
    }
    else if (auto table_step = dynamic_pointer_cast<TableScanStep>(node->getStep()))
    {
        if (const auto * filter = table_step->getPushdownFilterCast())
            collectProbesImpl(filter->getFilter(), node, res);
        if (auto query = dynamic_pointer_cast<ASTSelectQuery>(table_step->getQueryInfo().query))
        {
            if (auto query_filter = query->where())
                collectProbesImpl(query_filter, node, res);
            if (auto query_filter = query->prewhere())
                collectProbesImpl(query_filter, node, res);
        }
    }
    else if (auto cte_step = dynamic_pointer_cast<CTERefStep>(node->getStep()))
    {
        auto cte_id = cte_step->getId();
        if (auto it = visited_ctes.find(cte_id); it != visited_ctes.end())
            return;
        visited_ctes.insert(cte_id);
        collectImpl(cte_info.getCTEs().at(cte_id), cte_info, res, visited_ctes);
    }
    for (const auto & child : node->getChildren())
        collectImpl(child, cte_info, res, visited_ctes);
}

void RuntimeFilterCollector::collectProbesImpl(ConstASTPtr filter, PlanNodePtr node, RuntimeFilterBuildsAndProbes & res)
{
    for (const auto & runtime_filter : RuntimeFilterUtils::extractRuntimeFilters(filter).first)
    {
        auto id = RuntimeFilterUtils::extractId(runtime_filter);
        res.probes[id].insert(node);
    }
}

std::optional<CacheableRuntimeFilters> RuntimeFilterChecker::check(PlanNodePtr subtree)
{
    CacheableRuntimeFilters res;
    // a cacheable subtree does not contain CTERef step
    RuntimeFilterBuildsAndProbes subtree_runtime_filter_info = RuntimeFilterCollector::collect(subtree, {});
    if (subtree_runtime_filter_info.builds.empty() && subtree_runtime_filter_info.probes.empty())
        return std::make_optional(std::move(res));
    // not cacheable if the builder is probed outside the subtree
    for (const auto & build : subtree_runtime_filter_info.builds)
    {
        RuntimeFilterId id = build.first;
        auto it_query = query_runtime_filter_info.probes.find(id);
        auto it_subtree = subtree_runtime_filter_info.probes.find(id);
        if (it_query != query_runtime_filter_info.probes.end()
            && it_subtree != subtree_runtime_filter_info.probes.end()
            && it_query->second.size() == it_subtree->second.size())
        {
            res.ignorable.insert(id);
        } else
        {
            LOG_DEBUG(log, "runtime filter id {} is uncacheable because it is probed outside the subtree", id);
            return std::nullopt;
        }
    }
    // next check all builder involved recursively. the right tree of builder must be deterministic
    // if the right tree contains probes, this will be done recursively
    std::unordered_set<RuntimeFilterId> ids_to_check;
    for (const auto & probe : subtree_runtime_filter_info.probes)
        ids_to_check.insert(probe.first);
    bool cacheable = checkRecursive(ids_to_check, res);
    return cacheable ? std::make_optional(std::move(res)) : std::nullopt;
}

// on success, returns the extra runtime filter ids that should be checked
// on fail return std::nullopt
bool RuntimeFilterChecker::checkRuntimeFilterId(RuntimeFilterId id,
                                                std::unordered_set<RuntimeFilterId> & unchecked_ids,
                                                CacheableRuntimeFilters & checked)
{
    if (checked.isChecked(id))
        return true;
    auto builder = query_runtime_filter_info.builds.at(id);
    auto right_child = builder.node->getChildren()[1];
    if (!isSubtreeDeterministic(right_child, context))
    {
        LOG_DEBUG(log, "runtime filter id {} is uncacheable because its builder is unsupported");
        return false;
    }

    auto runtime_filters_in_right_child = RuntimeFilterCollector::collect(right_child, {});

    checked.cacheable.emplace(id, std::move(builder));
    for (const auto & pair : runtime_filters_in_right_child.probes)
    {
        if (!checked.isChecked(pair.first))
            unchecked_ids.insert(pair.first);
    }
    return true;
}

bool RuntimeFilterChecker::checkRecursive(std::unordered_set<RuntimeFilterId> & unchecked_ids, CacheableRuntimeFilters & checked)
{
    if (unchecked_ids.empty())
        return true;

    RuntimeFilterId id = *unchecked_ids.begin();
    unchecked_ids.erase(unchecked_ids.begin());

    auto check_success = checkRuntimeFilterId(id, unchecked_ids, checked);
    if (!check_success)
        return false;

    return checkRecursive(unchecked_ids, checked);
}


} // DB::CacheableChecker
