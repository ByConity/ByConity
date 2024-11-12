#pragma once

#include <Analyzers/QualifiedColumnName.h>
#include <Advisor/ColumnUsage.h>


namespace DB
{

/**
 * StatisticInfo:
 *  - sample_ndv: the ndv for a column in the particular part
 *  - total_rows: the total number of rows for the particular part
 *  - total_predicates: The total number of predicates involving the specified column, including in and equals and others
 */
struct StatisticInfo
{
    size_t sample_ndv{};
    size_t sample_row_num{};
    size_t total_predicates{};
};

/**
 * PredicateInfo:
 *  - PredicateExpressions: predicate_ast_expression - count
 *  - total: The total number of occurrences of a ColumnUsageType
 */
using PredicateExpressions = std::unordered_map<ConstASTPtr, size_t>;
struct PredicateInfo
{
    PredicateExpressions expressions;
    size_t total{};

    PredicateInfo() = default;
    PredicateInfo(PredicateExpressions & expressions_, size_t total_): expressions(std::move(expressions_)), total(total_) {}
};
using PredicateInfos = std::unordered_map<ColumnUsageType, PredicateInfo>;

enum class PotentialIndexType
{
    BLOOM_FILTER, // just support bloom_filter for skip index
    BITMAP_INDEX,
    SEGMENT_BITMAP_INDEX, // For high cordinality
    ALREADY_BITMAP_INDEX, // used in test, to find the column already has bitmap index
};

inline std::string toString(PotentialIndexType indexType)
{
    switch (indexType)
    {
        case PotentialIndexType::BLOOM_FILTER:
            return "BLOOM_FILTER";
        case PotentialIndexType::BITMAP_INDEX:
            return "BITMAP_INDEX";
        case PotentialIndexType::SEGMENT_BITMAP_INDEX:
            return "SEGMENT_BITMAP_INDEX";
        case PotentialIndexType::ALREADY_BITMAP_INDEX:
            return "ALREADY_BITMAP_INDEX";
        default:
            return "Unknown";
    }
}

struct PotentialColumnInfo
{
    PotentialIndexType index_type;
    StatisticInfo statistic_info;
    PredicateInfos predicate_infos;
};
using PotentialColumns = std::unordered_map<QualifiedColumnName, PotentialColumnInfo, QualifiedColumnNameHash>;

using PotentialPrewhereColumns = std::unordered_map<QualifiedColumnName, std::pair<Float64, Float64>, QualifiedColumnNameHash>;

struct IndexOverhead
{
    size_t hashs;
    size_t bits_per_rows;
    size_t uncompressed_index_size;
};

using IndexSelectors = std::vector<Float64>;
struct IndexEffect
{
    IndexSelectors index_selectors;
    size_t total_expressions;
};

struct PotentialIndex
{
    QualifiedColumnName column;
    PotentialIndexType index_type;
    Float32 false_positive_rate;

    IndexOverhead index_overhead;
    IndexEffect index_effect;

    StatisticInfo statistic_info;
    PredicateInfos predicate_infos;
};

}
