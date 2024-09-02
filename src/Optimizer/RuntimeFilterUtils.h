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
    RuntimeFilterDescription(RuntimeFilterId id_, ASTPtr expr_, double filter_factor_, std::vector<ASTPtr> partition_columns_exprs_)
        : id(id_), expr(std::move(expr_)), filter_factor(filter_factor_), partition_columns_exprs(std::move(partition_columns_exprs_))
    {
    }

    RuntimeFilterId id;
    ASTPtr expr;
    double filter_factor;
    std::vector<ASTPtr> partition_columns_exprs;
};

class SymbolStatistics;
using SymbolStatisticsPtr = std::shared_ptr<SymbolStatistics>;
class PlanNodeStatistics;
using PlanNodeStatisticsPtr = std::shared_ptr<PlanNodeStatistics>;
class FilterStep;

class RuntimeFilterUtils
{
public:
    static ConstASTPtr createRuntimeFilterExpression(
        RuntimeFilterId id, const std::string & symbol, const std::vector<String> & partition_columns, double filter_factor);
    static ConstASTPtr createRuntimeFilterExpression(const RuntimeFilterDescription & description);

    static RuntimeFilterId extractId(const ConstASTPtr & runtime_filter);
    static std::optional<RuntimeFilterDescription> extractDescription(const ConstASTPtr & runtime_filter);

    /* runtime_filters, static_filters */
    static std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> extractRuntimeFilters(const ConstASTPtr & conjuncts);
    static std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> extractExecutableRuntimeFiltersAndPush1stRf(const ConstASTPtr & conjuncts);
    static std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> extractExecutableRuntimeFilters(const ConstASTPtr & conjuncts);
    static bool isInternalRuntimeFilter(const ConstASTPtr & expr);
    static bool isInternalRuntimeFilter(const ASTFunction & function);
    static bool isExecutableRuntimeFilter(const ConstASTPtr & expr);

    static double estimateSelectivity(
        const ASTPtr & expr,
        const SymbolStatisticsPtr & build_stats,
        const PlanNodeStatisticsPtr & probe_stats,
        const NamesAndTypes & column_types,
        ContextMutablePtr & context);

    static std::vector<ASTPtr>
    createRuntimeFilterForFilter(const RuntimeFilterDescription & description, const String & query_id, bool only_bf);

    // RuntimeFilterBloomFilterExists will be at the end of std::vector<ASTPtr> .
    static std::vector<ASTPtr> createRuntimeFilterForTableScan(
        const RuntimeFilterDescription & description, const String & query_id, size_t wait_ms, bool need_bf, bool range_cover, bool & is_range_or_set, bool & has_bf);

    static std::vector<ASTPtr> generateFunctionArgs(const RuntimeFilterDescription & description, const String & query_id);
    static bool containsRuntimeFilters(const ConstASTPtr & filter);
    static std::vector<RuntimeFilterId> extractRuntimeFilterId(const ConstASTPtr & conjuncts);

    static ASTPtr removeAllInternalRuntimeFilters(ASTPtr expr);
};

}
