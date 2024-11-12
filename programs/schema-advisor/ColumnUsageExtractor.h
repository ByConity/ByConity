#pragma once

#include <Analyzers/QualifiedColumnName.h>
#include <Advisor/ColumnUsage.h>
#include <Interpreters/Context_fwd.h>

#include <string>
#include <utility>
#include <vector>

namespace DB
{

class ColumnUsageExtractor
{
public:
    // EqualityAndInUsages: 
    // for skip index: equality_usages, in_usages, total_predicates
    // for bitmap index: arraysetfunc_usages, {}, total_predicates
    using EqualityAndInUsages = std::tuple<std::vector<ColumnUsage>, std::vector<ColumnUsage>, size_t>;
    using ColumnToEqualityAndInUsages = std::unordered_map<QualifiedColumnName, EqualityAndInUsages, QualifiedColumnNameHash>;
    using ColumnToScannedUsages = std::unordered_map<QualifiedColumnName, size_t, QualifiedColumnNameHash>;

    // for prewhere: equality_usages, in_usages, range_usages, other_usages, total_predicates
    using PrewherePredicateUsages = std::tuple<std::vector<ColumnUsage>, std::vector<ColumnUsage>, std::vector<ColumnUsage>, std::vector<ColumnUsage>, size_t>;
    using ColumnToPrewherePredicateUsages = std::unordered_map<QualifiedColumnName, PrewherePredicateUsages, QualifiedColumnNameHash>;


    explicit ColumnUsageExtractor(ContextMutablePtr _context, size_t _max_threads): context(_context), max_threads(_max_threads) {}

    ColumnUsages extractColumnUsages(const std::vector<std::string> & queries) const;
    ColumnToEqualityAndInUsages extractUsageForSkipIndex(const ColumnUsages & column_usages) const;
    ColumnToPrewherePredicateUsages extractUsageForPrewhere(const ColumnUsages & column_usages) const;

    ColumnToScannedUsages extractUsageForLowCardinality(const ColumnUsages & column_usages) const;

private:
    ContextMutablePtr context;
    size_t max_threads;
    // which "in" filters are considered interesting
    static constexpr size_t IN_LIST_SIZE_UPPER_BOUND = 10;
    static constexpr float EQUALITY_AND_IN_PREDICATE_THRESHOLD = 0.5;
};

} // DB
