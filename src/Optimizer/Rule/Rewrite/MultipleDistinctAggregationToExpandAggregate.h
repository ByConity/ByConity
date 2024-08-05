#pragma once
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>
#include "Interpreters/Context_fwd.h"
#include "Optimizer/Rule/Pattern.h"

namespace DB
{

/**
 * This rule rewrites an aggregate query with multiple distinct aggregations 
 * into an expanded double aggregation in which the regular aggregation expressions 
 * and every distinct clause is aggregated in a separate group. The results are 
 * then combined in a second aggregate.
 * 
 * First example: query without filter clauses.
 *  
 * <pre>
 * Table : ORDERS [K, product, category, items] 
 * Query: 
 * SELECT 
 *   COUNT(*) as cnt, 
 *   SUM(items) as sum, 
 *   COUNT(DISTINCT product) as cnt_1, 
 *   COUNT(DISTINCT category) as cnt_2 
 * FROM ORDERS
 * GROUP BY K;
 * </pre>
 * 
 * This rewrite rule will transform query plan 
 * 
 * <pre>
 * - Aggregation
 *     key = GROUP BY (K)
 *     functions =  count(*) as cnt
 *                  sum(items) as sum      
 *                  count(DISTINCT product) as cnt_1
 *                  count(DISTINCT category) as cnt_2
 *     output = [K, cnt, sum, cnt_1, cnt_2]
 *        - TableScan [ORDERS]
 * </pre>
 * 
 * into query plan : 
 * 
 * <pre>
 * - Aggregation
 *        key = GROUP BY (K)
 *        functions = anyIf(cnt) FILTER (groupid = 0)
 *                    anyIf(sum) FILTER (groupid = 0)
 *                    countIf(product) FILTER (groupid = 1)
 *                    countIf(category) FILTER (groupid = 2)
 *        output = [K, cnt_1, cnt_2, cnt, sum]
 *     - Aggregation 
 *          key = GROUP BY (K, product, category, groupid)
 *          functions =   count(*)   as cnt
 *                        sum(items) as sum 
 *          output = [K, product, category, cnt, sum, groupid]
 *          - Expand (
 *              projection = [
 *                  (K, null,    null,     item, 0),
 *                  (K, product, null,     null, 1),
 *                  (K, null,    category, null, 2)
 *              ]
 *              output = [K, product, category, items, groupid]
 *                  - TableScan [ORDERS]
 * </pre>
 * 
 * Performance 
 *
 * If the number of distinct values is low then the number of shuffled rows 
 * can be very low even after the expand operator, so COUNT DISTINCT can be 
 * relatively fast due to the local partial aggregations.
 * 
 * If the number of distinct values is high and you use multiple COUNT DISTINCT 
 * for different columns or expressions in a single query then the number of 
 * shuffled rows can explode and become huge, partial aggregations cannot be 
 * effectively applied (few duplicate groups to reduce), so more executor memory 
 * can be required to complete the query successfully.
 */
class MultipleDistinctAggregationToExpandAggregate : public Rule
{
public:
    RuleType getType() const override { return RuleType::MULTIPLE_DISTINCT_AGG_TO_EXPAND_AGG; }
    String getName() const override { return "MULTIPLE_DISTINCT_AGG_TO_EXPAND_AGG"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_expand_distinct_optimization;
    }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

private:
    static const std::set<String> distinct_func;
    static const std::set<String> distinct_func_with_if;
    static const std::set<String> non_distinct_func_with_if;
    static const std::set<String> un_supported_func;
    static const std::unordered_map<String, String> distinct_func_normal_func;
    static bool hasNoFilterOrMask(const AggregatingStep & step);
    static bool hasMultipleDistincts(const AggregatingStep & step);
    static bool hasMixedDistinctAndNonDistincts(const AggregatingStep & step);
    static bool hasNoUnSupportedFunc(const AggregatingStep & step);

    /**
     * Distinct/Non-distinct aggregate function's arguments must unique.
     *
     * This rule will add a pre-compute aggregate with all distinct 
     * columns and group ID as the key, if non-distinct aggregate function's 
     * arguments repeated with distinct columns, result would be wrong.
     */
    static bool hasUniqueArgument(const AggregatingStep & step);

    static AggregateDescription distinctAggWithMask(
        const AggregateDescription & agg_desc, String & mask_column, Assignments & new_argument_assignments, ContextMutablePtr context);

    static AggregateDescription nonDistinctAggWithMask(const AggregateDescription & agg_desc, String & mask_column);

    static PlanNodePtr makeUnionNode(
        RuleContext & rule_context,
        std::set<Int32> & group_id_value,
        Assignments & assignments,
        NameToType & name_type,
        std::map<Int32, Names> & group_id_non_null_symbol,
        String & group_id_symbol,
        DataStream & output_stream,
        PlanNodePtr child);

    static PlanNodePtr makeCTENode(
        RuleContext & rule_context,
        std::set<Int32> & group_id_value,
        Assignments & assignments,
        NameToType & name_type,
        std::map<Int32, Names> & group_id_non_null_symbol,
        String & group_id_symbol,
        DataStream & output_stream,
        PlanNodePtr child);
};

}
