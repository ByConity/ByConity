#include <Interpreters/PartitionPredicateVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/pushFilterIntoStorage.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>


namespace DB
{

ASTPtr pushFilterIntoStorage(ASTPtr query_filter, StoragePtr storage, SelectQueryInfo & query_info, ContextPtr context)
{
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);
    const auto & settings = context->getSettingsRef();

    /// Set partition_filter
    auto * merge_tree_data = dynamic_cast<MergeTreeMetaBase *>(storage.get());
    if (merge_tree_data && settings.enable_partition_filter_push_down)
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

    /// Set query.prewhere()
    if (merge_tree_data && select_query->where() && settings.enable_optimizer_early_prewhere_push_down)
    {
        /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
        if (const auto & column_sizes = merge_tree_data->getColumnSizes(); !column_sizes.empty())
        {
            /// Extract column compressed sizes.
            std::unordered_map<std::string, UInt64> column_compressed_sizes;
            for (const auto & [name, sizes] : column_sizes)
                column_compressed_sizes[name] = sizes.data_compressed;

            auto current_info = buildSelectQueryInfoForQuery(query_info.query, context);
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
