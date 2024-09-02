#include <vector>
#include <Optimizer/RuntimeFilterUtils.h>

#include <Functions/FunctionsRuntimeFilter.h>
#include <Functions/InternalFunctionRuntimeFilter.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>
#include <Common/Exception.h>

namespace DB
{
ConstASTPtr RuntimeFilterUtils::createRuntimeFilterExpression(
    RuntimeFilterId id, const std::string & symbol, const std::vector<String> & partition_columns, double filter_factor)
{
    auto partition_columns_tuple = std::make_shared<ASTFunction>();
    partition_columns_tuple->name = partition_columns.empty() ? "array" : "tuple";
    partition_columns_tuple->arguments = std::make_shared<ASTExpressionList>();
    for (const auto & column : partition_columns)
        partition_columns_tuple->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(column));
    partition_columns_tuple->children.push_back(partition_columns_tuple->arguments);

    return makeASTFunction(
        InternalFunctionRuntimeFilter::name,
        std::make_shared<ASTLiteral>(id),
        std::make_shared<ASTIdentifier>(symbol),
        std::make_shared<ASTLiteral>(filter_factor),
        partition_columns_tuple);
}

ConstASTPtr RuntimeFilterUtils::createRuntimeFilterExpression(const RuntimeFilterDescription & description)
{
    auto partition_columns_tuple = std::make_shared<ASTFunction>();
    partition_columns_tuple->name = description.partition_columns_exprs.empty() ? "array" : "tuple";
    partition_columns_tuple->arguments = std::make_shared<ASTExpressionList>();
    partition_columns_tuple->arguments->children = description.partition_columns_exprs;
    partition_columns_tuple->children.push_back(partition_columns_tuple->arguments);

    return makeASTFunction(
        InternalFunctionRuntimeFilter::name,
        std::make_shared<ASTLiteral>(description.id),
        description.expr,
        std::make_shared<ASTLiteral>(description.filter_factor),
        partition_columns_tuple);
}

bool RuntimeFilterUtils::containsRuntimeFilters(const ConstASTPtr & filter)
{
    if (!filter)
        return false;

    for (const auto & child : filter->children)
        if (containsRuntimeFilters(child))
            return true;

    if (filter->getType() == ASTType::ASTFunction)
    {
        const auto * function = filter->as<ASTFunction>();
        return function->name == InternalFunctionRuntimeFilter::name || function->name == RuntimeFilterBloomFilterExists::name;
    }

    return false;
}

bool RuntimeFilterUtils::isExecutableRuntimeFilter(const ConstASTPtr & expr)
{
    return expr && expr->getType() == ASTType::ASTFunction && expr->as<ASTFunction &>().name == RuntimeFilterBloomFilterExists::name;
}

bool RuntimeFilterUtils::isInternalRuntimeFilter(const ConstASTPtr & expr)
{
    return expr && expr->getType() == ASTType::ASTFunction && expr->as<ASTFunction &>().name == InternalFunctionRuntimeFilter::name;
}

bool RuntimeFilterUtils::isInternalRuntimeFilter(const ASTFunction & function)
{
    return function.name == InternalFunctionRuntimeFilter::name;
}

double RuntimeFilterUtils::estimateSelectivity(
    const ASTPtr & expr,
    const SymbolStatisticsPtr & build_stats,
    const PlanNodeStatisticsPtr & probe_stats,
    const NamesAndTypes & column_types,
    ContextMutablePtr & context)
{
    auto stats = probe_stats->copy();
    ASTPtr constructed_runtime_filter = makeASTFunction(
        "and",
        makeASTFunction("greaterOrEquals", expr, std::make_shared<ASTLiteral>(build_stats->getMin())),
        makeASTFunction("lessOrEquals", expr, std::make_shared<ASTLiteral>(build_stats->getMax())));
    auto selectivity = FilterEstimator::estimateFilterSelectivity(stats, constructed_runtime_filter, column_types, context);

    if (const auto * identifier = expr->as<ASTIdentifier>())
    {
        auto ndv_selectivity = static_cast<double>(build_stats->getNdv()) / probe_stats->getSymbolStatistics(identifier->name())->getNdv();
        selectivity = std::min(selectivity, ndv_selectivity);
    }
    return selectivity;
}

std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> RuntimeFilterUtils::extractExecutableRuntimeFiltersAndPush1stRf(const ConstASTPtr & conjuncts)
{
    std::vector<ConstASTPtr> runtime_filters;
    std::vector<ConstASTPtr> static_filters;
    if (!conjuncts)
        return std::make_pair(runtime_filters, static_filters);
    bool pushed = false;
    for (auto & filter : PredicateUtils::extractConjuncts(conjuncts))
        if (isExecutableRuntimeFilter(filter)) {
            if (!pushed) {
                static_filters.emplace_back(filter);
                pushed = true;
            } else {
                runtime_filters.emplace_back(filter);
            }
        }
        else
            static_filters.emplace_back(filter);
    return std::make_pair(runtime_filters, static_filters);
}

std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> RuntimeFilterUtils::extractExecutableRuntimeFilters(const ConstASTPtr & conjuncts)
{
    std::vector<ConstASTPtr> runtime_filters;
    std::vector<ConstASTPtr> static_filters;
    if (!conjuncts)
        return std::make_pair(runtime_filters, static_filters);

    for (auto & filter : PredicateUtils::extractConjuncts(conjuncts))
        if (isExecutableRuntimeFilter(filter))
            runtime_filters.emplace_back(filter);
        else
            static_filters.emplace_back(filter);
    return std::make_pair(runtime_filters, static_filters);
}

std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> RuntimeFilterUtils::extractRuntimeFilters(const ConstASTPtr & conjuncts)
{
    std::vector<ConstASTPtr> runtime_filters;
    std::vector<ConstASTPtr> static_filters;
    if (!conjuncts)
        return std::make_pair(runtime_filters, static_filters);

    for (auto & filter : PredicateUtils::extractConjuncts(conjuncts))
        if (isInternalRuntimeFilter(filter))
            runtime_filters.emplace_back(filter);
        else
            static_filters.emplace_back(filter);
    return std::make_pair(runtime_filters, static_filters);
}

std::vector<RuntimeFilterId> RuntimeFilterUtils::extractRuntimeFilterId(const ConstASTPtr & conjuncts)
{
    std::vector<RuntimeFilterId> ids;
    if (!conjuncts)
        return ids;

    for (auto & filter : PredicateUtils::extractConjuncts(conjuncts))
        if (isInternalRuntimeFilter(filter))
            ids.emplace_back(extractId(filter));

    return ids;
}

RuntimeFilterId RuntimeFilterUtils::extractId(const ConstASTPtr & runtime_filter)
{
    Utils::checkArgument(isInternalRuntimeFilter(runtime_filter));

    auto function = runtime_filter->as<ASTFunction &>();
    auto id = function.arguments->children[0];
    return id->as<ASTLiteral &>().value.get<RuntimeFilterId>();
}

std::optional<RuntimeFilterDescription> RuntimeFilterUtils::extractDescription(const ConstASTPtr & runtime_filter)
{
    if (runtime_filter->getType() != ASTType::ASTFunction)
        return {};

    auto function = runtime_filter->as<ASTFunction &>();
    if (function.name != InternalFunctionRuntimeFilter::name)
        return {};

    auto id = function.arguments->children[0]->as<ASTLiteral &>().value.get<RuntimeFilterId>();
    auto expr = function.arguments->children[1];
    auto filter_factor = function.arguments->children[2]->as<ASTLiteral &>().value.get<double>();
    ASTs partition_columns_exprs;
    // empty partition_columns_exprs may be fold as empty literal
    if (auto * fucntion = function.arguments->children[3]->as<ASTFunction>())
        partition_columns_exprs = fucntion->arguments->children;

    return RuntimeFilterDescription{static_cast<RuntimeFilterId>(id), expr, filter_factor, partition_columns_exprs};
}

std::vector<ASTPtr>
RuntimeFilterUtils::createRuntimeFilterForFilter(const RuntimeFilterDescription & description, const String & query_id, bool only_bf)
{
    if (only_bf)
    {
        auto key = RuntimeFilterManager::makeKey(query_id, description.id);
        return {
            ASTFunction::makeASTFunctionWithVectorArgs(RuntimeFilterBloomFilterExists::name, generateFunctionArgs(description, query_id))};
    }

    bool is_range_or_set, has_bf;
    return createRuntimeFilterForTableScan(description, query_id, 0, true, true, is_range_or_set, has_bf);
}

std::vector<ASTPtr> RuntimeFilterUtils::generateFunctionArgs(const DB::RuntimeFilterDescription & description, const DB::String & query_id)
{
    std::vector<ASTPtr> args;
    args.emplace_back(std::make_shared<ASTLiteral>(RuntimeFilterManager::makeKey(query_id, description.id)));
    args.emplace_back(description.expr);
    args.insert(args.end(), description.partition_columns_exprs.begin(), description.partition_columns_exprs.end());
    return args;
}

// RuntimeFilterBloomFilterExists will be at the end of std::vector<ASTPtr> .
std::vector<ASTPtr> RuntimeFilterUtils::createRuntimeFilterForTableScan(
    const RuntimeFilterDescription & description,
    const String & query_id,
    size_t wait_ms,
    bool need_bf,
    bool range_cover,
    bool & is_range_or_set,
    bool & has_bf)
{
    auto key = RuntimeFilterManager::makeKey(query_id, description.id);
    auto dynamic_value = RuntimeFilterManager::getInstance().getDynamicValue(key);
    const auto & value = dynamic_value->get(wait_ms);
    std::vector<ASTPtr> res;
    if (dynamic_value->isReady())
    {
        if (value.bypass == BypassType::BYPASS_LARGE_HT)
            return {};
        else if (value.bypass == BypassType::BYPASS_EMPTY_HT)
        {
            is_range_or_set = true;
            return {std::make_shared<ASTLiteral>(0)};
        }

        if (value.is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(value.data);
            if (d.is_bf)
            {
                if (d.bloom_filter->has_min_max && range_cover && d.bloom_filter->isRangeMatch())
                {
                    is_range_or_set = true;
                    res.emplace_back(
                        makeASTFunction("greaterOrEquals", description.expr, std::make_shared<ASTLiteral>(d.bloom_filter->min())));
                    res.emplace_back(
                        makeASTFunction("lessOrEquals", description.expr, std::make_shared<ASTLiteral>(d.bloom_filter->max())));
                }
                else
                {
                    if (d.bloom_filter->has_min_max)
                    {
                        res.emplace_back(
                            makeASTFunction("greaterOrEquals", description.expr, std::make_shared<ASTLiteral>(d.bloom_filter->min())));
                        res.emplace_back(
                            makeASTFunction("lessOrEquals", description.expr, std::make_shared<ASTLiteral>(d.bloom_filter->max())));
                    }
                    if (need_bf) {
                        has_bf = true;
                        res.emplace_back(ASTFunction::makeASTFunctionWithVectorArgs(
                            RuntimeFilterBloomFilterExists::name,
                            generateFunctionArgs(description, query_id)));
                    }
                }
            }
            else if (d.values_set)
            {
                is_range_or_set = true;
                if (d.values_set->has_min_max && range_cover && d.values_set->isRangeMatch())
                {
                    res.emplace_back(makeASTFunction("greaterOrEquals", description.expr, std::make_shared<ASTLiteral>(d.values_set->min)));
                    res.emplace_back(makeASTFunction("lessOrEquals", description.expr, std::make_shared<ASTLiteral>(d.values_set->max)));
                }
                else
                {
                    if (!d.values_set->isRangeAlmostMatch()) {
                        Array array;
                        for (auto & v : d.values_set->set)
                        {
                            array.emplace_back(v);
                        }
                        res.emplace_back(makeASTFunction("in", description.expr,
                                                        std::make_shared<ASTLiteral>(std::move(array))));
                    }

                    if (d.values_set->has_min_max)
                    {
                        res.emplace_back(
                            makeASTFunction("greaterOrEquals", description.expr, std::make_shared<ASTLiteral>(d.values_set->min)));
                        res.emplace_back(
                            makeASTFunction("lessOrEquals", description.expr, std::make_shared<ASTLiteral>(d.values_set->max)));
                    }
                }
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "unknown runtime filter type");
            }
        }
        else
        {
            auto const & d = std::get<InternalDynamicData>(value.data);
            if (!d.set.isNull())
            {
                
                is_range_or_set = true;
                const auto & array = d.set.get<Array>();

                bool bypass = false;
                if (!d.range.isNull())
                {
                    const auto & range_array = d.range.get<Array>();
                    auto min_value = range_array[0];
                    auto max_value = range_array[1];
                    if (isRangeAlmostMatchSet(min_value, max_value, array.size())) {
                        bypass = true;
                    }
                }

                if (!bypass)
                    res.emplace_back(makeASTFunction("in", description.expr, std::make_shared<ASTLiteral>(array)));
            }
            
            if (!d.range.isNull())
            {
                if (d.bf.isNull())
                    is_range_or_set = true;

                const auto & array = d.range.get<Array>();
                auto min_value = array[0];
                auto max_value = array[1];
                res.emplace_back(makeASTFunction("greaterOrEquals", description.expr, std::make_shared<ASTLiteral>(min_value)));
                res.emplace_back(makeASTFunction("lessOrEquals", description.expr, std::make_shared<ASTLiteral>(max_value)));
            }

             if (!d.bf.isNull() && need_bf)
            {
                has_bf = true;
                res.emplace_back(ASTFunction::makeASTFunctionWithVectorArgs(
                    RuntimeFilterBloomFilterExists::name,
                    generateFunctionArgs(description, query_id)));
            }
        }
    }
    else
    {
        // only enable bloom
        has_bf = true;
        // auto key = RuntimeFilterManager::makeKey(query_id, description.id);
        res.emplace_back(
            ASTFunction::makeASTFunctionWithVectorArgs(RuntimeFilterBloomFilterExists::name, generateFunctionArgs(description, query_id)));
    }

    return res;
}

ASTPtr RuntimeFilterUtils::removeAllInternalRuntimeFilters(ASTPtr expr)
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

}
