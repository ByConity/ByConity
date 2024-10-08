#include <Advisor/Rules/PartitionKeyAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Parsers/formatAST.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <Optimizer/Property/Property.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>

namespace DB
{

class PartitionKeyAdvise : public IWorkloadAdvise
{
public:
    PartitionKeyAdvise(
        QualifiedTableName table_,
        ASTPtr original_cluster_by_key_,
        String original_column_,
        String new_column_,
        double benefit_)
        : table(std::move(table_)), original_cluster_by_key(original_cluster_by_key_)
        , original_column(std::move(original_column_)), new_column(std::move(new_column_)), benefit(benefit_)
    {
    }

    String apply(WorkloadTables & tables) override
    {
        if (original_column.empty()) {
            return "original table is not a bucket table, the bucket number should be determined";
        }
        
        if (original_column == new_column)
            return "original column is already optimal";
        
        WorkloadTablePtr optimized_table = tables.getTable(table);
        auto cluster_by_key = optimized_table->getClusterByKey();
        if (!ASTEquality::compareTree(cluster_by_key, original_cluster_by_key))
            return "duplicate modification to the table ddl";

        String msg;
        if (cluster_by_key && !cluster_by_key->as<ASTIdentifier>())
            msg = "original cluster by is nontrivial";
        optimized_table->updateClusterByKey(std::make_shared<ASTIdentifier>(new_column));
        return msg;
    }

    QualifiedTableName getTable() override { return table; }
    String getAdviseType() override { return "Cluster By"; }
    String getOriginalValue() override { return original_column; }
    String getOptimizedValue() override { return new_column; }
    double getBenefit() override {return benefit; }

private:
    QualifiedTableName table;
    ASTPtr original_cluster_by_key;
    String original_column;
    String new_column;
    double benefit;
};

WorkloadAdvises PartitionKeyAdvisor::analyze(AdvisorContext & context) const
{
    if (enable_memo_based_advise)
        return memoBasedAdvise(context);
    else
        return frequencyBasedAdvise(context);
}

WorkloadAdvises PartitionKeyAdvisor::frequencyBasedAdvise(AdvisorContext & context) const
{
    std::unordered_map<QualifiedTableName, std::unordered_map<String, size_t>> column_usage_by_table;
    for (const auto & [qualified_column, metrics] : context.column_usages)
    {
        auto shuffle_freq = metrics.getFrequency(ColumnUsageType::EQUI_JOIN, /*only_source_table=*/true);
        if (shuffle_freq > 0 && isValidColumn(qualified_column, context))
            column_usage_by_table[qualified_column.getQualifiedTable()][qualified_column.column] += shuffle_freq;
    }

    WorkloadAdvises res{};
    for (const auto & [table, column_freq] : column_usage_by_table)
    {
        auto max_column_freq = *std::max_element(column_freq.begin(), column_freq.end(),
                                                 [](const auto & p1, const auto & p2) {
                                                     // enforce unique ordering
                                                     if (p1.second == p2.second)
                                                         return p1.first < p2.first;
                                                     return p1.second < p2.second;
                                                 });
        auto optimized_table = context.tables.tryGetTable(table);
        auto cluster_by_key = optimized_table ? optimized_table->getClusterByKey() : nullptr;
        auto original_column = (cluster_by_key) ? serializeAST(*cluster_by_key) : String{};
        res.emplace_back(std::make_shared<PartitionKeyAdvise>(table, cluster_by_key, original_column, max_column_freq.first, max_column_freq.second));
    }
    return res;
}

bool PartitionKeyAdvisor::isValidColumn(const QualifiedColumnName & column, AdvisorContext & context) const
{
    auto column_type = context.getColumnType(column);
    if (!column_type || !column_type->isValueRepresentedByInteger()) // sharding key only accepts integers
    {
        LOG_DEBUG(log, "Column {}.{}.{} is not a valid sharding key, because it is not an integer type", column.database, column.table, column.column);
        return false;
    }

    return true;
}


std::vector<QualifiedColumnName> PartitionKeyAdvisor::getSortedInterestingColumns(AdvisorContext & context) const
{
    std::vector<std::pair<QualifiedColumnName, size_t>> interesting_columns;
    for (const auto & [qualified_column, metrics] : context.column_usages)
    {
        auto join_freq = metrics.getFrequency(ColumnUsageType::EQUI_JOIN, /*only_source_table*/false);
        if (join_freq > 0 && isValidColumn(qualified_column, context))
            interesting_columns.emplace_back(std::make_pair(qualified_column, join_freq));
    }

    std::sort(interesting_columns.begin(), interesting_columns.end(), [](const auto & p1, const auto & p2) -> bool {
        // enforce unique order: by cost desc then by name asc
        if (p1.second == p2.second)
            return p1.first < p2.first;
        return p1.second > p2.second;
    });
    std::vector<QualifiedColumnName> res{};
    for (auto & entry : interesting_columns)
        res.emplace_back(std::move(entry.first));
    return res;
}

class TableLayoutSearcher
{
public:
    explicit TableLayoutSearcher(AdvisorContext & context_,
                                 const std::vector<QualifiedColumnName> & interesting_columns_,
                                 size_t max_iteration_ = 150)
        : context(context_)
        , interesting_columns(interesting_columns_)
        , max_iteration(max_iteration_)
    {
        // calculate lower bounds, * partition means every table can use its desired partition in every query
        for (const auto & column : interesting_columns)
            lower_bound_layout.emplace(column.getQualifiedTable(), WorkloadTablePartitioning::starPartition());
        tables_to_search = lower_bound_layout.size();
        lower_bound_cost = calculateWorkloadCost(lower_bound_layout);
        // calculate upper bounds, empty means every table use its current partition
        optimal_layout = {}; // empty set refers to original layout
        optimal_cost = calculateWorkloadCost({});
    }

    TableLayout getOptimalLayout() const { return optimal_layout; }
    double getOptimalCost() const { return optimal_cost; }
    TableLayout getLowerBoundLayout() const { return lower_bound_layout; }
    double getLowerBoundCost() const { return lower_bound_cost; }

    // calculate the workload total cost under given table layout using what-if cascades search
    double calculateWorkloadCost(const TableLayout & table_layout);
    // recursively search and update optimal_layout/cost until max_iteration is reached
    void searchFrom(TableLayout & table_layout, size_t & iteration, size_t & tables_depth);

private:
    AdvisorContext & context;
    const std::vector<QualifiedColumnName> & interesting_columns;
    const size_t max_iteration;

    TableLayout lower_bound_layout;
    double lower_bound_cost;
    size_t tables_to_search;

    TableLayout optimal_layout;
    double optimal_cost;
    std::unordered_set<TableLayout, TableLayoutHash> explored;

    LoggerPtr log = getLogger("PartitionKeyAdvisor");
};


WorkloadAdvises PartitionKeyAdvisor::memoBasedAdvise(AdvisorContext & context) const
{
    std::vector<QualifiedColumnName> interesting_columns = getSortedInterestingColumns(context);

    TableLayoutSearcher searcher(context, interesting_columns);

    double lower_bound_cost = searcher.getLowerBoundCost();
    double original_cost = searcher.getOptimalCost();
    double gap_for_improve = original_cost - lower_bound_cost;

    LOG_DEBUG(log, "search space: {} tables, {} columns", searcher.getLowerBoundLayout().size(), interesting_columns.size());
    LOG_DEBUG(log, "lower bound cost: {}", lower_bound_cost);
    LOG_DEBUG(log, "upper bound cost: {}", original_cost);
    LOG_DEBUG(log, "gap for improvement: {}", gap_for_improve);
    LOG_DEBUG(log, "maximum improvement ratio: {}", gap_for_improve / original_cost);

    TableLayout table_layout = searcher.getLowerBoundLayout();
    size_t iteration = 0;
    size_t tables_depth = 0;
    searcher.searchFrom(table_layout, iteration, tables_depth);

    double optimal_cost = searcher.getOptimalCost();
    double actual_improvement = original_cost - optimal_cost;

    if (actual_improvement <= 0)
    {
        LOG_DEBUG(log, "no better layout found");
        return {};
    }

    LOG_DEBUG(log, "new layout cost: {}", optimal_cost);
    LOG_DEBUG(log, "actual improvement ratio: {}", actual_improvement / gap_for_improve);

    TableLayout optimal_layout = searcher.getOptimalLayout();
    WorkloadAdvises res{};
    for (const auto & [table, partitioning] : optimal_layout)
    {
        if (partitioning.isStarPartitioned())
            continue;
        const auto & partition_key = partitioning.getPartitionKey();

        auto optimized_table = context.tables.tryGetTable(table);
        auto cluster_by_key = optimized_table ? optimized_table->getClusterByKey() : nullptr;
        auto original_column = (cluster_by_key) ? serializeAST(*cluster_by_key) : String{};
        res.emplace_back(std::make_shared<PartitionKeyAdvise>(table, cluster_by_key, original_column, partition_key.column, actual_improvement));
    }

    return res;
}

double TableLayoutSearcher::calculateWorkloadCost(const TableLayout & table_layout)
{
    std::atomic<double> res{0.0};

    for (auto & query : context.queries)
    {
        auto thread_group = CurrentThread::getGroup();
        context.query_thread_pool.scheduleOrThrowOnError(
            [&]{
                setThreadName("Advisor");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                double query_cost = query->getOptimalCost(table_layout);
                // workaround for fetch_and_add
                auto current = res.load();
                while (!res.compare_exchange_weak(current, current + query_cost));
            });
    }
    context.query_thread_pool.wait();
    
    return res.load();
}

void TableLayoutSearcher::searchFrom(TableLayout & table_layout, size_t & iteration, size_t & tables_depth)
{
    for (const auto & column : interesting_columns) // in order
    {
        if (iteration > max_iteration)
            return;

        const auto table = column.getQualifiedTable();

        if (!table_layout[table].isStarPartitioned()) // already partitioned
            continue;

        // search the branch where table is partitioned by this column
        table_layout[table] = WorkloadTablePartitioning{column};
        if (explored.contains(table_layout))
        {
            LOG_DEBUG(log, "Iteration {}, depth {} [skipped]: duplicate table layout (visited: {})",
                      iteration, tables_depth, explored.size());
            // revert
            table_layout[table] = WorkloadTablePartitioning::starPartition();
            continue;
        }

        // new table layout
        ++iteration;
        ++tables_depth;
        double cost = calculateWorkloadCost(table_layout);
        bool is_leaf = (tables_depth == tables_to_search);

        if (cost < optimal_cost && is_leaf)
        {
            LOG_DEBUG(log, "Iteration {}, Leaf [better]: better layout found with cost {} < {}", iteration, cost, optimal_cost);
            optimal_cost = cost;
            optimal_layout = table_layout;
        }
        else if (cost < optimal_cost && !is_leaf)
        {
            LOG_DEBUG(log, "Iteration {}, depth {} [candidate]: expanding", iteration, tables_depth);
            searchFrom(table_layout, iteration, tables_depth);
        }
        else if (cost >= optimal_cost && is_leaf)
        {
            LOG_DEBUG(log, "Iteration {}, Leaf [worse]: worse layout found with cost {} >= {}", iteration, cost, optimal_cost);
        }
        else // (cost >= optimal_cost && !is_leaf)
        {
            LOG_DEBUG(log, "Iteration {}, depth {} [prune]: pruned branch with cost {} >= {}", iteration, tables_depth, cost, optimal_cost);
        }

        // finish current branch and revert
        explored.emplace(table_layout);
        table_layout[table] = WorkloadTablePartitioning::starPartition();
        --tables_depth;
    }
}

}
