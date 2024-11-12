#include <Advisor/Rules/OrderByKeyAdvise.h>

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
#include <utility>
#include <vector>

namespace DB
{

class OrderByKeyAdvise : public IWorkloadAdvise
{
public:
    OrderByKeyAdvise(
        QualifiedTableName table_,
        ASTPtr original_order_by_,
        String original_column_,
        String new_column_,
        double benefit_,
        std::vector<std::pair<String, double>> candidates_)
        : table(std::move(table_))
        , original_order_by(original_order_by_)
        , original_column(std::move(original_column_))
        , new_column(std::move(new_column_))
        , benefit(benefit_)
        , candidates(std::move(candidates_))
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
    std::vector<std::pair<String, double>> getCandidates() override { return candidates; }

private:
    QualifiedTableName table;
    ASTPtr original_order_by;
    String original_column;
    String new_column;
    double benefit;
    std::vector<std::pair<String, double>> candidates;
};

WorkloadAdvises OrderByKeyAdvisor::analyze(AdvisorContext & context) const
{
    std::unordered_map<QualifiedTableName, std::unordered_map<String, double>> column_usage_by_table;
    for (const auto & [qualified_column, metrics] : context.column_usages)
    {
        auto predicate_freq = metrics.getFrequency(ColumnUsageType::EQUALITY_PREDICATE, /*only_source_table=*/true)
            + metrics.getFrequency(ColumnUsageType::IN_PREDICATE, /*only_source_table=*/true)
            + metrics.getFrequency(ColumnUsageType::RANGE_PREDICATE, /*only_source_table=*/true)
            + metrics.getFrequency(ColumnUsageType::EQUI_JOIN, /*only_source_table=*/true) /* runtime_filter*/;
        if (predicate_freq > 0 && isValidColumn(qualified_column, context))
            column_usage_by_table[qualified_column.getQualifiedTable()][qualified_column.column] += predicate_freq;
    }

    WorkloadAdvises res{};
    for (const auto & [table, column_freq] : column_usage_by_table)
    {
        std::vector<std::pair<String, double>> sorted_freq{column_freq.begin(), column_freq.end()};
        std::sort(sorted_freq.begin(), sorted_freq.end(), [](const auto & p1, const auto & p2) {
            // enforce unique ordering
            if (p1.second == p2.second)
                return p1.first > p2.first;
            return p1.second > p2.second;
        });
        if (sorted_freq.size() > 3)
            sorted_freq.resize(3);

        auto optimized_table = context.tables.tryGetTable(table);
        auto order_by = optimized_table ? optimized_table->getOrderBy() : nullptr;
        auto original_column = (order_by) ? serializeAST(*order_by) : String{};
        res.emplace_back(
            std::make_shared<OrderByKeyAdvise>(table, order_by, original_column, sorted_freq[0].first, sorted_freq[0].second, sorted_freq));
    }
    return res;
}

bool OrderByKeyAdvisor::isValidColumn(const QualifiedColumnName & /*column*/, AdvisorContext & /*context*/) const
{
    // auto column_type = context.getColumnType(column);
    // if (!column_type || !column_type->isValueRepresentedByInteger()) // sharding key only accepts integers
    // {
    //     LOG_DEBUG(log, "Column {}.{}.{} is not a valid sharding key, because it is not an integer type", column.database, column.table, column.column);
    //     return false;
    // }
    return true;
}

}
