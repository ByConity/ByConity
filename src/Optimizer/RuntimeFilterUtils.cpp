#include <Optimizer/RuntimeFilterUtils.h>

#include <Functions/FunctionsRuntimeFilter.h>
#include <Functions/InternalFunctionRuntimeFilter.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Common/Exception.h>

namespace DB
{
ConstASTPtr RuntimeFilterUtils::createRuntimeFilterExpression(RuntimeFilterId id, const std::string & symbol)
{
    return makeASTFunction(
        InternalFunctionRuntimeFilter::name,
        std::make_shared<ASTLiteral>(id),
        std::make_shared<ASTIdentifier>(symbol),
        std::make_shared<ASTLiteral>(0.));
}

ConstASTPtr RuntimeFilterUtils::createRuntimeFilterExpression(const RuntimeFilterDescription & description)
{
    return makeASTFunction(
        InternalFunctionRuntimeFilter::name,
        std::make_shared<ASTLiteral>(description.id),
        description.expr->clone(),
        std::make_shared<ASTLiteral>(description.filter_factor));
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

bool RuntimeFilterUtils::isInternalRuntimeFilter(const ConstASTPtr & expr)
{
    return expr && expr->getType() == ASTType::ASTFunction && expr->as<ASTFunction &>().name == InternalFunctionRuntimeFilter::name;
}

double RuntimeFilterUtils::estimateSelectivity(
    const RuntimeFilterDescription & description,
    const SymbolStatisticsPtr & filter_source,
    const PlanNodeStatisticsPtr & child_stats,
    const FilterStep & step,
    ContextMutablePtr & context)
{
    auto stats = child_stats->copy();
    const auto & names_and_types = step.getInputStreams()[0].header.getNamesAndTypes();
    ConstASTPtr constructed_runtime_filter = makeASTFunction(
        "and",
        makeASTFunction("greaterOrEquals", description.expr->clone(), std::make_shared<ASTLiteral>(filter_source->getMin())),
        makeASTFunction("lessOrEquals", description.expr->clone(), std::make_shared<ASTLiteral>(filter_source->getMax())));
    auto selectivity = FilterEstimator::estimateFilterSelectivity(stats, constructed_runtime_filter, names_and_types, context);

    if (const auto * identifier = description.expr->as<ASTIdentifier>())
    {
        auto ndv_selectivity
            = static_cast<double>(filter_source->getNdv()) / child_stats->getSymbolStatistics(identifier->name())->getNdv();
        selectivity = std::min(selectivity, ndv_selectivity);
    }
    return selectivity;
}

std::pair<std::vector<ConstASTPtr>, std::vector<ConstASTPtr>> RuntimeFilterUtils::extractRuntimeFilters(const ConstASTPtr & conjuncts)
{
    std::vector<ConstASTPtr> runtime_filters;
    std::vector<ConstASTPtr> static_filters;
    Utils::checkArgument(conjuncts.get());
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
    Utils::checkArgument(conjuncts.get());
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
    return RuntimeFilterDescription{static_cast<RuntimeFilterId>(id), expr, filter_factor};
}

std::vector<ASTPtr> RuntimeFilterUtils::createRuntimeFilterForFilter(
    const RuntimeFilterDescription & description, const String & query_id)
{
    auto key = RuntimeFilterManager::makeKey(query_id, description.id);
    return {makeASTFunction(
            RuntimeFilterBloomFilterExists::name,
            std::make_shared<ASTLiteral>(key),
            description.expr->clone())};

        //     return {makeASTFunction(RuntimeFilterRange::name, std::make_shared<ASTLiteral>(key), description.expr->clone())};
        //     return {makeASTFunction(RuntimeFilterInValues::name, std::make_shared<ASTLiteral>(key), description.expr->clone())};
}

std::vector<ASTPtr> RuntimeFilterUtils::createRuntimeFilterForTableScan(
    const RuntimeFilterDescription & description, const String & query_id, size_t wait_ms, bool need_bf, bool range_cover)
{
    auto dynamic_key = RuntimeFilterManager::makeKey(query_id, description.id);
    auto dynamic_value = RuntimeFilterManager::getInstance().getDynamicValue(dynamic_key);
    const auto & value = dynamic_value->get(wait_ms);
    std::vector<ASTPtr> res;
    if (dynamic_value->isReady())
    {
        if (value.bypass == BypassType::BYPASS_LARGE_HT)
            return {};
        else if (value.bypass == BypassType::BYPASS_EMPTY_HT)
            return {std::make_shared<ASTLiteral>(0)}; //makeASTFunction("equals", std::make_shared<ASTLiteral>(0), std::make_shared<ASTLiteral>(1))

        if (value.is_local)
        {
            auto const & d = std::get<RuntimeFilterVal>(value.data);
            if (d.is_bf)
            {
                if (d.bloom_filter->has_min_max && range_cover && d.bloom_filter->isRangeMatch())
                {
                    res.emplace_back(makeASTFunction("greaterOrEquals", description.expr->clone(),
                                                     std::make_shared<ASTLiteral>(d.bloom_filter->Min())));
                    res.emplace_back(makeASTFunction("lessOrEquals", description.expr->clone(),
                                                     std::make_shared<ASTLiteral>(d.bloom_filter->Max())));
                }
                else
                {
                    auto key = RuntimeFilterManager::makeKey(query_id, description.id);
                    if (need_bf)
                        res.emplace_back(makeASTFunction(
                            RuntimeFilterBloomFilterExists::name,
                            std::make_shared<ASTLiteral>(key),
                            description.expr->clone()));
                    if (d.bloom_filter->has_min_max)
                    {
                        res.emplace_back(makeASTFunction("greaterOrEquals", description.expr->clone(),
                                                        std::make_shared<ASTLiteral>(d.bloom_filter->Min())));
                        res.emplace_back(makeASTFunction("lessOrEquals", description.expr->clone(),
                                                        std::make_shared<ASTLiteral>(d.bloom_filter->Max())));
                    }
                }
            }
            else if (d.values_set)
            {
                if (d.values_set->has_min_max && range_cover && d.values_set->isRangeMatch())
                {
                    res.emplace_back(makeASTFunction("greaterOrEquals", description.expr->clone(),
                                                     std::make_shared<ASTLiteral>(d.values_set->min)));
                    res.emplace_back(makeASTFunction("lessOrEquals", description.expr->clone(),
                                                     std::make_shared<ASTLiteral>(d.values_set->max)));
                }
                else
                {
                    Array array;
                    for (auto & v : d.values_set->set)
                    {
                        array.emplace_back(v);
                    }
                    res.emplace_back(makeASTFunction("in", description.expr->clone(),
                                                    std::make_shared<ASTLiteral>(std::move(array))));

                    if (d.values_set->has_min_max)
                    {
                        res.emplace_back(makeASTFunction("greaterOrEquals", description.expr->clone(),
                                                        std::make_shared<ASTLiteral>(d.values_set->min)));
                        res.emplace_back(makeASTFunction("lessOrEquals", description.expr->clone(),
                                                        std::make_shared<ASTLiteral>(d.values_set->max)));
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
                const auto & array = d.set.get<Array>();
                res.emplace_back(makeASTFunction("in", description.expr->clone(), std::make_shared<ASTLiteral>(array)));
            }

            if (!d.bf.isNull() && need_bf)
            {
                auto key = RuntimeFilterManager::makeKey(query_id, description.id);
                res.emplace_back(makeASTFunction(
                    RuntimeFilterBloomFilterExists::name,
                    std::make_shared<ASTLiteral>(key),
                    description.expr->clone()));
            }
            if (!d.range.isNull())
            {
                const auto & array = d.range.get<Array>();
                auto min_value = array[0];
                auto max_value = array[1];
                res.emplace_back(makeASTFunction("greaterOrEquals", description.expr->clone(), std::make_shared<ASTLiteral>(min_value)));
                res.emplace_back(makeASTFunction("lessOrEquals", description.expr->clone(), std::make_shared<ASTLiteral>(max_value)));
            }
        }
    }
    else
    {
        // only enable bloom
        auto key = RuntimeFilterManager::makeKey(query_id, description.id);
        res.emplace_back(makeASTFunction(
            RuntimeFilterBloomFilterExists::name,
            std::make_shared<ASTLiteral>(key),
            description.expr->clone()));
    }

    return res;
}

}
