#include "ColumnUsageExtractor.h"
#include "SchemaAdvisorHelpers.h"
#include "MockEnvironment.h"

#include <Advisor/ColumnUsage.h>
#include <Advisor/WorkloadQuery.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/formatAST.h>
#include <Storages/KeyDescription.h>
#include <boost/algorithm/string/replace.hpp>
#include <bthread/mutex.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

namespace
{
    WorkloadQueries buildWorkloadQueriesCollectException(const std::vector<std::string> & queries,
                                                         ContextPtr from_context,
                                                         ThreadPool & query_thread_pool,
                                                         MessageCollector & collector)
    {
        WorkloadQueries res(queries.size());
        for (size_t i = 0; i < queries.size(); ++i)
        {
            query_thread_pool.scheduleOrThrowOnError([&, i] {
                setThreadName("BuildQuery");
                try
                {
                    res[i] = WorkloadQuery::build("q" + std::to_string(i), queries[i], from_context);
                }
                catch (...)
                {
                    std::string msg = "failed to build query " + std::to_string(i) + "\nreason: " + getCurrentExceptionMessage(true)
                        + "\nsql: " + queries[i] + "\n";
                    collector.collect(std::move(msg));
                }
            });
        }
        query_thread_pool.wait();
        res.erase(std::remove(res.begin(), res.end(), nullptr), res.end());
        return res;
    }
}

ColumnUsages ColumnUsageExtractor::extractColumnUsages(const std::vector<std::string> & queries) const
{
    ThreadPool query_thread_pool{std::min<size_t>(max_threads, queries.size())};
    MessageCollector collector;
    WorkloadQueries workload_queries = buildWorkloadQueriesCollectException(queries, context, query_thread_pool, collector);
    if (queries.empty())
        throw Exception("No valid query has been extracted", ErrorCodes::BAD_ARGUMENTS);

    LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Successfully planed {} / {} queries", workload_queries.size(), queries.size());
    collector.logCollectedError();

    return buildColumnUsages(workload_queries);
}

ColumnUsageExtractor::ColumnToScannedUsages ColumnUsageExtractor::extractUsageForLowCardinality(const ColumnUsages & column_usages) const
{
    ColumnToScannedUsages res;
    for (const auto & [column, info] : column_usages)
    {
        if (MockEnvironment::isPrimaryKey(column, context))
        {
            LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Column {} skipped because it is a primary key", column.getFullName());
            continue;
        }

        auto scanned = info.getUsages(ColumnUsageType::SCANNED, /*only_source_table=*/false);
        if (scanned.empty())
            continue;
    
        res.emplace(column, scanned.size());
    }

    return res;
}

ColumnUsageExtractor::ColumnToEqualityAndInUsages ColumnUsageExtractor::extractUsageForSkipIndex(const ColumnUsages & column_usages) const
{
    /// if only interested in a specific table, do it here
    // std::erase_if(column_usages, [&](const auto & pair) { return pair.first.database != database || pair.first.table != table;});

    ColumnToEqualityAndInUsages res;
    for (const auto & [column, info] : column_usages)
    {
        if (MockEnvironment::isPrimaryKey(column, context))
        {
            LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Column {} skipped because it is a primary key", column.getFullName());
            continue;
        }

        size_t arraysetfunc_count = info.getFrequency(ColumnUsageType::ARRAY_SET_FUNCTION, /*only_source_table=*/false);
        size_t others_count = info.getFrequency(ColumnUsageType::OTHER_PREDICATE, /*only_source_table=*/false);

        if (arraysetfunc_count)
        {
            auto arraysetfuncs = info.getUsages(ColumnUsageType::ARRAY_SET_FUNCTION, /*only_source_table=*/false);
            size_t total_count = arraysetfuncs.size() + others_count;
            /// TODO: Optimize the ColumnToEqualityAndInUsages struct?
            res.emplace(column, std::make_tuple(std::move(arraysetfuncs), std::vector<ColumnUsage>{}, total_count));

            continue;
        }

        auto equalities = info.getUsages(ColumnUsageType::EQUALITY_PREDICATE, /*only_source_table=*/false);
        auto ins = info.getUsages(ColumnUsageType::IN_PREDICATE, /*only_source_table=*/false);
        size_t ranges_count = info.getFrequency(ColumnUsageType::RANGE_PREDICATE, /*only_source_table=*/false);

        size_t total_count = equalities.size() + ins.size() + ranges_count + others_count;
        if (total_count == 0)
        {
            LOG_DEBUG(
                getLogger("ColumnUsageExtractor"),
                "Column {} skipped, total count: {}",
                column.getFullName(),
                total_count);
            continue;
        }

        // Keep the set size threshold limit on
        // Remove in lists whose in set size is larger than IN_LIST_SIZE_UPPER_BOUND
        std::erase_if(ins, [](const ColumnUsage & usage) {
            if (auto func = dynamic_pointer_cast<const ASTFunction>(usage.expression); func && func->name == "in")
                if (auto expr_list = dynamic_pointer_cast<const ASTExpressionList>(func->arguments); expr_list && expr_list->children.size() == 2)
                    if (auto tuple = dynamic_pointer_cast<const ASTFunction>(expr_list->children[1]); tuple && tuple->name == "tuple")
                        if (auto tuple_expr = dynamic_pointer_cast<const ASTExpressionList>(tuple->arguments))
                            return tuple_expr->children.size() > IN_LIST_SIZE_UPPER_BOUND;
            return true;
        });

        size_t eq_in_count = equalities.size() + ins.size();
        if (eq_in_count == 0)
        {
            LOG_DEBUG(
                getLogger("ColumnUsageExtractor"),
                "Column {} skipped, eq & in count: {}, total count: {}",
                column.getFullName(),
                eq_in_count,
                total_count);
            continue;
        }

        // Temply loosen the restrictions of in+equals proportion
        if (eq_in_count * 1.0 < total_count * EQUALITY_AND_IN_PREDICATE_THRESHOLD)
        {
            LOG_DEBUG(
                getLogger("ColumnUsageExtractor"),
                "Column {} maybe skipped, eq & in count: {}, total count: {}",
                column.getFullName(),
                eq_in_count,
                total_count);
            continue;
        }

        LOG_DEBUG(
            getLogger("ColumnUsageExtractor"),
            "Column {} added, eq & in count: {}, total count: {}",
            column.getFullName(),
            eq_in_count,
            total_count);

        res.emplace(column, std::make_tuple(std::move(equalities), std::move(ins), total_count));
    }
    return res;
}

ColumnUsageExtractor::ColumnToPrewherePredicateUsages ColumnUsageExtractor::extractUsageForPrewhere(const ColumnUsages & column_usages) const
{
    /// if only interested in a specific table, do it here
    // std::erase_if(column_usages, [&](const auto & pair) { return pair.first.database != database || pair.first.table != table;});

    ColumnToPrewherePredicateUsages res;
    for (const auto & [column, info] : column_usages)
    {
        if (MockEnvironment::isPrimaryKey(column, context))
        {
            LOG_DEBUG(getLogger("ColumnUsageExtractor"), "Column {} skipped because it is a primary key", column.getFullName());
            continue;
        }

        auto equalities = info.getUsages(ColumnUsageType::EQUALITY_PREDICATE, /*only_source_table=*/false);
        auto ins = info.getUsages(ColumnUsageType::IN_PREDICATE, /*only_source_table=*/false);
        auto ranges = info.getUsages(ColumnUsageType::RANGE_PREDICATE, /*only_source_table=*/false);
        auto others = info.getUsages(ColumnUsageType::OTHER_PREDICATE, /*only_source_table=*/false);

        size_t total_count = equalities.size() + ins.size() + ranges.size() + others.size();

        if (total_count == 0)
            continue;

        // Keep the set size threshold limit on
        // Remove in lists whose in set size is larger than IN_LIST_SIZE_UPPER_BOUND
        std::erase_if(ins, [](const ColumnUsage & usage) {
            if (auto func = dynamic_pointer_cast<const ASTFunction>(usage.expression); func && func->name == "in")
                if (auto expr_list = dynamic_pointer_cast<const ASTExpressionList>(func->arguments); expr_list && expr_list->children.size() == 2)
                    if (auto tuple = dynamic_pointer_cast<const ASTFunction>(expr_list->children[1]); tuple && tuple->name == "tuple")
                        if (auto tuple_expr = dynamic_pointer_cast<const ASTExpressionList>(tuple->arguments))
                            return tuple_expr->children.size() > IN_LIST_SIZE_UPPER_BOUND;
            return true;
        });

        res.emplace(column, std::make_tuple(std::move(equalities), std::move(ins), std::move(ranges), std::move(others), total_count));
    }
    return res;
}

} // DB
