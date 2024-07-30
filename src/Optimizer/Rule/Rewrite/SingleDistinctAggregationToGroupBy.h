#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{
/**
 * Implements distinct aggregations with similar inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT s0, s1, ...),
 *        F2(DISTINCT s0, s1, ...),
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *          GROUP BY (k)
 *          F1(x)
 *          F2(x)
 *      - Aggregation
 *             GROUP BY (k, s0, s1, ...)
 *          - X
 * </pre>
 * <p>
 * Assumes s0, s1, ... are symbol references (i.e., complex expressions have been pre-projected)
 */
class SingleDistinctAggregationToGroupBy : public Rule
{
public:
    RuleType getType() const override { return RuleType::SINGLE_DISTINCT_AGG_TO_GROUPBY; }
    String getName() const override { return "SINGLE_DISTINCT_AGG_TO_GROUPBY"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_single_distinct_to_group_by;
    }
    ConstRefPatternPtr getPattern() const override;

protected:
    static const std::set<String> distinct_func;
    static const std::unordered_map<String, String> distinct_func_normal_func;

    // All Aggregate Functions must have distinct key word
    static bool allDistinctAggregates(const AggregatingStep & step);

    // All Aggregate Functions must have same arguments
    static bool hasSingleDistinctInput(const AggregatingStep & step);

    // All Aggregate Functions must can't contains mask
    static bool noMasks(const AggregatingStep & step);

    // All Count Aggregate Functions must have at most one argument.
    static bool allCountHasAtMostOneArguments(const AggregatingStep & step);

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}
