#include <Advisor/Rules/ColumnUsageAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/ColumnUsage.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadTable.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>

#include <algorithm>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class ColumnUsageAdvise : public IWorkloadAdvise
{
public:
    ColumnUsageAdvise(QualifiedTableName table_, String column_, std::vector<std::pair<String, double>> candidates_)
        : table(std::move(table_)), column(std::move(column_)), candidates(std::move(candidates_))
    {
    }

    String apply(WorkloadTables &) override { return "not implement"; }

    QualifiedTableName getTable() override { return table; }
    std::optional<String> getColumnName() override { return column; }
    String getAdviseType() override { return "Column Usage"; }
    String getOriginalValue() override { return ""; }
    String getOptimizedValue() override { return ""; }
    double getBenefit() override { return 0; }
    std::vector<std::pair<String, double>> getCandidates() override { return candidates; }

private:
    QualifiedTableName table;
    String column;
    std::vector<std::pair<String, double>> candidates;
};

WorkloadAdvises ColumnUsageAdvisor::analyze(AdvisorContext & context) const
{
    std::map<QualifiedColumnName, std::unordered_map<String, double>> column_usage_by_table;
    for (const auto & [qualified_column, metrics] : context.column_usages)
    {
        for (const auto & [type, count] : metrics.getFrequencies(true))
        {
            column_usage_by_table[qualified_column][toString(type)] += count;
        }
    }

    WorkloadAdvises res;
    for (const auto & [table, column_freq] : column_usage_by_table)
    {
        std::vector<std::pair<String, double>> sorted_freq{column_freq.begin(), column_freq.end()};
        std::sort(sorted_freq.begin(), sorted_freq.end(), [](const auto & p1, const auto & p2) {
            // enforce unique ordering
            if (p1.second == p2.second)
                return p1.first < p2.first;
            return p1.second < p2.second;
        });

        res.emplace_back(std::make_shared<ColumnUsageAdvise>(table.getQualifiedTable(), table.column, sorted_freq));
    }
    return res;
}

}
