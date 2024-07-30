#pragma once
#include <utility>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{

/**
 * This rule rewrite cross join with filter to union.
 *
 * For example.
 * 
 * <pre>
 * SELECT count(*)
 * FROM lineitem
 * INNER JOIN orders ON (l_orderkey = o_orderkey) OR (l_partkey = o_custkey)
 * </pre>
 *
 * This rule will transform query plan: 
 * 
 * <pre>
 * - Inner Join 
 *    keys = empty
 *    filters = (l_orderkey = o_orderkey) OR (l_partkey = o_custkey) 
 *    - TableScan lineitem
 *    - TableScan orders
 * </pre>
 * 
 * To query plan 
 * 
 * <pre>
 * - Union ALL
 *   - Filter 
 *     predicate = (l_orderkey = o_orderkey) 
 *        - Inner Join 
 *          keys = empty   
 *          filters = empty
 *          - TableScan lineitem
 *          - TableScan orders
 *   - Filter
 *     predicate = NOT(l_partkey = o_custkey) and (l_partkey = o_custkey) 
 *        - Inner Join 
 *          keys = empty
 *          filters = empty
 *          - TableScan lineitem
 *          - TableScan orders
 *</pre>
 *
 * After predicate pushdown, cross-join might be converted to inner-join.
 */
class CrossJoinToUnion : public Rule
{
public:
    RuleType getType() const override { return RuleType::CROSS_JOIN_TO_UNION; }
    String getName() const override { return "CrossJoinToUnion"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_cross_join_to_union; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

private:
    static bool matchPattern(const JoinStep & step);
    static bool hasDNFFilters(const ConstASTPtr & filter);
    static std::pair<ConstASTPtr, ConstASTPtr> splitFilter(const ConstASTPtr & filter);
};

}
