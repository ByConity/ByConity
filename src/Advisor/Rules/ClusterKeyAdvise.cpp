#include <Advisor/Rules/ClusterKeyAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/ColumnUsage.h>
#include <Advisor/WorkloadTable.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Types.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>

#include <algorithm>
#include <string>
#include <unordered_map>

namespace DB
{

class ClusterKeyAdvise : public IWorkloadAdvise
{
public:
    ClusterKeyAdvise(QualifiedTableName table_,
                     ASTPtr original_order_by_,
                     String original_column_,
                     String new_column_,
                     double benefit_)
        : table(std::move(table_)), original_order_by(original_order_by_)
        , original_column(std::move(original_column_)), new_column(std::move(new_column_)), benefit(benefit_)
    {
    }

    String apply(WorkloadTables & tables) override
    {
        if (original_column == new_column)
            return "original column is already optimal";

        WorkloadTablePtr optimized_table = tables.getTable(table);
        auto order_by = optimized_table->getOrderBy();
        if (!ASTEquality::compareTree(order_by, original_order_by))
            return "duplicate modification to the table ddl";

        String msg;
        if (order_by && !order_by->as<ASTIdentifier>())
            msg = "original order by is nontrivial";
        optimized_table->updateOrderBy(std::make_shared<ASTIdentifier>(new_column));
        return msg;
    }

    QualifiedTableName getTable() override { return table; }
    String getAdviseType() override { return "Order By"; }
    String getOriginalValue() override { return original_column; }
    String getOptimizedValue() override { return new_column; }
    double getBenefit() override { return benefit; }

private:
    QualifiedTableName table;
    ASTPtr original_order_by;
    String original_column;
    String new_column;
    double benefit;
};

WorkloadAdvises ClusterKeyAdvisor::analyze(AdvisorContext & context) const
{
    std::unordered_map<QualifiedTableName, std::unordered_map<String, size_t>> column_usage_by_table;
    for (const auto & [qualified_column, metrics] : context.column_usages)
    {
        auto predicate_freq = metrics.getFrequency(ColumnUsageType::EQUALITY_PREDICATE, /*only_source_table=*/true)
                               + metrics.getFrequency(ColumnUsageType::RANGE_PREDICATE, /*only_source_table=*/true);
        if (predicate_freq > 0 && isValidColumn(qualified_column, context))
            column_usage_by_table[qualified_column.getQualifiedTable()][qualified_column.column] += predicate_freq;
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
        auto order_by = optimized_table ? optimized_table->getOrderBy() : nullptr;
        auto original_column = (order_by) ? serializeAST(*order_by) : String{};
        res.emplace_back(std::make_shared<ClusterKeyAdvise>(table, order_by, original_column, max_column_freq.first, max_column_freq.second));
    }
    return res;
}

bool ClusterKeyAdvisor::isValidColumn(const QualifiedColumnName & column, AdvisorContext & context) const
{
    auto column_type = context.getColumnType(column);
    if (!column_type || !column_type->isComparable()) // sharding key only accepts integers
    {
        LOG_DEBUG(log, "Column {}.{}.{} is not a valid order by key, because it is not comparable",
                  column.database, column.table, column.column);
        return false;
    }
    return true;
}

}
