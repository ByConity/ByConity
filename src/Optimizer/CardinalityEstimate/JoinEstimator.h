#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
class JoinEstimator
{
public:
    /**
     * The number of rows of A inner join B on A.k1 = B.k1 is estimated by this basic formula:
     * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)),
     * where V is the number of distinct values (ndv) of that column. The underlying assumption for
     * this formula is: each value of the smaller domain is included in the larger domain.
     *
     * Generally, inner join with multiple join keys can be estimated based on the above formula:
     * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
     * However, the denominator can become very large and excessively reduce the result, so we use a
     * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
     *
     * That is, join estimation is based on the most selective join keys. We follow this strategy
     * when different types of column statistics are available. E.g., if card1 is the cardinality
     * estimated by ndv of join key A.k1 and B.k1, card2 is the cardinality estimated by histograms
     * of join key A.k2 and B.k2, then the result cardinality would be min(card1, card2).
     *
     * @return join cardinality
     */
    static PlanNodeStatisticsPtr estimate(
        PlanNodeStatisticsPtr & left_stats,
        PlanNodeStatisticsPtr & right_stats,
        const JoinStep & join_step,
        bool enable_pk_fk,
        bool is_left_base_table = false,
        bool is_right_base_table = false);

    static PlanNodeStatisticsPtr computeCardinality(
        PlanNodeStatistics & left_stats,
        PlanNodeStatistics & right_stats,
        const Names & left_keys,
        const Names & right_keys,
        ASTTableJoin::Kind kind,
        bool enable_pk_fk,
        bool is_left_base_table = false,
        bool is_right_base_table = false);

private:
    static bool matchPKFK(UInt64 left_rows, UInt64 right_rows, UInt64 left_ndv, UInt64 right_ndv);
    static bool matchFKPK(UInt64 left_rows, UInt64 right_rows, UInt64 left_ndv, UInt64 right_ndv);

    static UInt64 computeCardinalityByFKPK(
        UInt64 fk_rows,
        UInt64 fk_ndv,
        UInt64 pk_ndv,
        PlanNodeStatistics & fk_stats,
        PlanNodeStatistics & pk_stats,
        SymbolStatistics & fk_key_stats,
        SymbolStatistics & pk_key_stats,
        String fk_key,
        String pk_key,
        bool is_left_base_table,
        bool is_right_base_table,
        std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics);

    static UInt64 computeCardinalityByHistogram(
        PlanNodeStatistics & left_stats,
        PlanNodeStatistics & right_stats,
        SymbolStatistics & left_key_stats,
        SymbolStatistics & right_key_stats,
        ASTTableJoin::Kind kind,
        String left_key,
        String right_key,
        std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics);

    static UInt64 computeCardinalityByNDV(
        PlanNodeStatistics & left_stats,
        PlanNodeStatistics & right_stats,
        SymbolStatistics & left_key_stats,
        SymbolStatistics & right_key_stats,
        ASTTableJoin::Kind kind,
        String left_key,
        String right_key,
        std::unordered_map<String, SymbolStatisticsPtr> & join_output_statistics);
};

}
