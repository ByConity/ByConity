#include "PushFilterToStorage.h"

#include <tuple>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PartitionPredicateVisitor.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
PushFilterToStorage::PushFilterToStorage(ConstStoragePtr storage_, ContextPtr local_context)
    : DB::WithContext(local_context), storage(storage_)
{
}

std::tuple<ASTs, ASTs> PushFilterToStorage::extractPartitionFilter(ASTPtr query_filter, bool supports_parition_runtime_filter)
{
    ASTs push_predicates;
    ASTs remain_predicates;
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);
    Names partition_key_names = storage->getInMemoryMetadataPtr()->getPartitionKey().column_names;
    Names virtual_key_names = storage->getVirtuals().getNames();
    partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
    auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
        if (!supports_parition_runtime_filter && RuntimeFilterUtils::isInternalRuntimeFilter(predicate))
            return false;

        PartitionPredicateVisitor::Data visitor_data{getContext(), std::const_pointer_cast<IStorage>(storage), predicate};
        PartitionPredicateVisitor(visitor_data).visit(predicate);
        return visitor_data.getMatch();
    });

    push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
    remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());
    return std::make_tuple(push_predicates, remain_predicates);
}

std::tuple<ASTs, ASTs> PushFilterToStorage::extractPrewhereWithStats(ASTPtr query_filter, PlanNodeStatisticsPtr storage_statistics)
{
    size_t max_active_prewhere_size = getContext()->getSettings().max_active_prewhere_size;
    float max_active_prewhere_selectivity = getContext()->getSettings().max_active_prewhere_selectivity;
    std::vector<std::pair<ASTPtr, double>> pre_conjuncts_with_selectivity;
    ASTs pre_conjuncts;
    ASTs where_conjuncts;
    ASTs full_conjuncts = PredicateUtils::extractConjuncts(query_filter);
    IdentifierNameSet used_columns;
    query_filter->collectIdentifierNames(used_columns);
    const auto & columns_desc = storage->getInMemoryMetadataPtr()->getColumns();
    NamesAndTypes names_and_types;
    for (const auto & col_name : used_columns)
        names_and_types.emplace_back(columns_desc.getPhysical(col_name));

    for (const auto & conjunct : full_conjuncts)
    {
        double selectivity = FilterEstimator::estimateFilterSelectivity(storage_statistics, conjunct, names_and_types, getContext());
        LOG_DEBUG(logger, "[OptimizerActivePrewhere] conjunct=" + serializeAST(*conjunct) + ", selectivity=" + std::to_string(selectivity));

        if (selectivity <= max_active_prewhere_selectivity)
            pre_conjuncts_with_selectivity.emplace_back(conjunct, selectivity);
        else
            where_conjuncts.push_back(conjunct);
    }

    // use the conjuncts with lowest selectivities;
    if (pre_conjuncts_with_selectivity.size() > max_active_prewhere_size)
    {
        std::sort(pre_conjuncts_with_selectivity.begin(), pre_conjuncts_with_selectivity.end(), [](const auto & x, const auto & y) {
            return x.second < y.second;
        });
        for (size_t i = max_active_prewhere_size; i < pre_conjuncts_with_selectivity.size(); i++)
        {
            auto [conjunct, selectivity] = pre_conjuncts_with_selectivity.back();
            pre_conjuncts_with_selectivity.pop_back();
            where_conjuncts.push_back(conjunct);
        }
    }
    for (auto c : pre_conjuncts_with_selectivity)
    {
        pre_conjuncts.emplace_back(std::move(c.first));
    }
    return std::make_tuple(pre_conjuncts, where_conjuncts);
}



}
