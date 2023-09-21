#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
struct RuntimeFilterDescription
{
    RuntimeFilterDescription(RuntimeFilterId id_, ConstASTPtr expr_, double filter_factor_)
        : id(id_), expr(std::move(expr_)), filter_factor(filter_factor_)
    {
    }

    RuntimeFilterId id;
    ConstASTPtr expr;
    double filter_factor;
};

class SymbolStatistics;
using SymbolStatisticsPtr = std::shared_ptr<SymbolStatistics>;
class PlanNodeStatistics;
using PlanNodeStatisticsPtr = std::shared_ptr<PlanNodeStatistics>;
class FilterStep;

/**
 * Runtime Filter, also as runtime filter, improve the performance of queries with selective joins
 * by filtering data early that would be filtered by join condition.
 *
 * When runtime Filtering is enabled, values are collected from the build side of join, and sent to
 * probe side of join in runtime.
 *
 * Runtime Filter could be used for dynamic partition pruning, reduce table scan data with index,
 * reduce exchange shuffle, and so on.
 *
 * Runtime Filter has two parts in plan:
 *  1. build side, model as projection attribute.
 *  2. consumer side, model as a filter predicates.
 */
class RuntimeFilterUtils
{
public:
    static ConstASTPtr createRuntimeFilterExpression(RuntimeFilterId id, const std::string & symbol);
    static ConstASTPtr createRuntimeFilterExpression(const RuntimeFilterDescription & description);

    static RuntimeFilterId extractId(const ConstASTPtr & runtime_filter);
    static std::optional<RuntimeFilterDescription> extractDescription(const ConstASTPtr & runtime_filter);

    /* runtime_filters, static_filters */
    static std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> extractRuntimeFilters(const ConstASTPtr & conjuncts);
    static bool isInternalRuntimeFilter(const ConstASTPtr & expr);

    static double estimateSelectivity(
        const RuntimeFilterDescription & description,
        const SymbolStatisticsPtr & filter_source,
        const PlanNodeStatisticsPtr & child_stats,
        const FilterStep & step,
        ContextMutablePtr & context);

    static std::vector<ASTPtr>
    createRuntimeFilterForFilter(const RuntimeFilterDescription & description, const String & query_id);

    static std::vector<ASTPtr> createRuntimeFilterForTableScan(
        const RuntimeFilterDescription & description, const String & query_id, size_t wait_ms, bool need_bf, bool range_cover);

    static bool containsRuntimeFilters(const ConstASTPtr & filter);
    static std::vector<RuntimeFilterId> extractRuntimeFilterId(const ConstASTPtr & conjuncts);
};

}
