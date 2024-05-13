#include <Interpreters/PartitionPredicateVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/pushFilterIntoStorage.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/PredicateUtils.h>


namespace DB
{

ASTPtr pushFilterIntoStorage(ASTPtr query_filter, StoragePtr storage, SelectQueryInfo & query_info, PlanNodeStatisticsPtr storage_statistics, const NamesAndTypes & names_and_types, ContextMutablePtr context)
{
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);
    const auto & settings = context->getSettingsRef();

    /// Set partition_filter
    auto * merge_tree_data = dynamic_cast<MergeTreeMetaBase *>(storage.get());
    if (merge_tree_data && merge_tree_data->supportsPrewhere() && settings.enable_partition_filter_push_down)
    {
        ASTs push_predicates;
        ASTs remain_predicates;

        Names partition_key_names = merge_tree_data->getInMemoryMetadataPtr()->getPartitionKey().column_names;
        Names virtual_key_names = merge_tree_data->getSampleBlockWithVirtualColumns().getNames();
        partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
        auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
            PartitionPredicateVisitor::Data visitor_data{context, partition_key_names};
            PartitionPredicateVisitor(visitor_data).visit(predicate);
            return visitor_data.getMatch();
        });

        push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
        remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());

        ASTPtr new_partition_filter;

        if (query_info.partition_filter)
        {
            push_predicates.push_back(query_info.partition_filter);
            new_partition_filter = PredicateUtils::combineConjuncts(push_predicates);
        }
        else
        {
            new_partition_filter = PredicateUtils::combineConjuncts<false>(push_predicates);
        }

        if (!PredicateUtils::isTruePredicate(new_partition_filter))
            query_info.partition_filter = std::move(new_partition_filter);

        conjuncts.swap(remain_predicates);
    }

    auto * select_query = query_info.getSelectQuery();

    /// Set query.where()
    {
        ASTPtr new_where;

        if (auto prev_where = select_query->where())
        {
            ASTs new_conjuncts = conjuncts;
            new_conjuncts.push_back(prev_where);
            new_where = PredicateUtils::combineConjuncts(new_conjuncts);
        }
        else
        {
            new_where = PredicateUtils::combineConjuncts<false>(conjuncts);
        }

        if (!PredicateUtils::isTruePredicate(new_where))
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_where));
    }
    
    // Choose a better subset of where to set the prewhere structure in query
    if (storage_statistics && select_query->where() && !select_query->prewhere())
    {
        std::vector<ASTPtr> pre_conjuncts;

        for (const auto & conjunct : PredicateUtils::extractConjuncts(select_query->getWhere()))
        {
            double selectivity = FilterEstimator::estimateFilterSelectivity(storage_statistics, conjunct, names_and_types, context);
            if (selectivity < context->getSettingsRef().max_active_prewhere_selectivity)
                pre_conjuncts.push_back(conjunct);

            if (pre_conjuncts.size() >= context->getSettingsRef().max_active_prewhere_size)
                break;
            LOG_DEBUG(&Poco::Logger::get("OptimizerActivePrewhere"), "conjunct=" + serializeAST(*conjunct) + ", selectivity=" + std::to_string(selectivity));
        }

        if (!pre_conjuncts.empty())
            select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, PredicateUtils::combineConjuncts(pre_conjuncts));
    }

    /// Set query.prewhere()
    if (merge_tree_data && merge_tree_data->supportsPrewhere() && select_query->where() && settings.enable_optimizer_early_prewhere_push_down)
    {
        /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
        if (const auto & column_sizes = merge_tree_data->getColumnSizes(); !column_sizes.empty())
        {
            /// Extract column compressed sizes.
            std::unordered_map<std::string, UInt64> column_compressed_sizes;
            for (const auto & [name, sizes] : column_sizes)
                column_compressed_sizes[name] = sizes.data_compressed;

            auto current_info = buildSelectQueryInfoForQuery(query_info.query, context);
            for (const auto & column_name : current_info.syntax_analyzer_result->requiredSourceColumns())
            {
                UInt64 size = merge_tree_data->getColumnCompressedSize(column_name);
                // Now get implicit column size only for prewhere pushdown
                if (size == 0 && context->getSettingsRef().enable_implicit_column_prewhere_push && isMapImplicitKey(column_name))
                {
                    size = merge_tree_data->calculateMapColumnSizesImpl(column_name).data_compressed;
                }
                column_compressed_sizes[column_name] = size;
            }            
            MergeTreeWhereOptimizer{
                current_info,
                context,
                std::move(column_compressed_sizes),
                merge_tree_data->getInMemoryMetadataPtr(),
                current_info.syntax_analyzer_result->requiredSourceColumns(),
                &Poco::Logger::get("pushFilterIntoStorage")};
        }
    }

    return select_query->where() ? select_query->where() : PredicateConst::TRUE_VALUE;
}
}
