#pragma once

#include <Advisor/WorkloadQuery.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Optimizer/CostModel/CostCalculator.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
class ColumnUsageInfo;
using ColumnUsages = std::unordered_map<QualifiedColumnName, ColumnUsageInfo, QualifiedColumnNameHash>;
ColumnUsages buildColumnUsages(const WorkloadQueries & queries);

enum class ColumnUsageType
{
    SCANNED, // columns scanned in a TableScanStep, used to decide hot columns
    EQUI_JOIN, // columns in "=" connected join conditions
    NON_EQUI_JOIN, // columns in ">" or "<" connected join conditions
    GROUP_BY, // columns in "group by" clause
    EQUALITY_PREDICATE, // columns in "= literal" filters
    IN_PREDICATE, // columns in "in list" filters
    RANGE_PREDICATE, // columns in "> literal" or "< literal" filters
    ARRAY_SET_FUNCTION, // columns in "has" or "arraySetCheck"
    OTHER_PREDICATE, // columns in "column ???" filters
};

String toString(ColumnUsageType type);

struct ColumnUsage
{
    ColumnUsageType type;
    PlanNodePtr node; // the node where the usage appears
    QualifiedColumnName column;
    ConstASTPtr expression; // the direct expression of usage, will be set for predicates
};

class ColumnUsageInfo
{
public:
    using TypeToUsageInfo = std::unordered_multimap<ColumnUsageType, ColumnUsage>;

    void update(ColumnUsage usage, bool is_source_table);

    size_t getFrequency(ColumnUsageType type, bool only_source_table = false) const;
    std::unordered_map<ColumnUsageType, size_t> getFrequencies(bool only_source_table = false) const;
    std::vector<ColumnUsage> getUsages(ColumnUsageType type, bool only_source_table = false) const;

private:
    TypeToUsageInfo usages_only_source_table;
    TypeToUsageInfo usages_non_source_table;
};

}
