#pragma once

#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/PlanVisitor.h>

namespace DB
{

/**
 * Extract correlation symbol from filter node in subquery.
 */
class Correlation
{
public:
    static std::vector<String> prune(PlanNodePtr & node, const Names & origin_correlation);
    static bool containsCorrelation(PlanNodePtr & node, Names & correlation);
    static bool isCorrelated(ConstASTPtr & expression, Names & correlation);
    static bool isUnreferencedScalar(PlanNodePtr & node);
};

struct DecorrelationResult
{
    PlanNodePtr node;
    std::set<String> symbols_to_propagate{};
    std::vector<ConstASTPtr> correlation_predicates{};
    bool at_most_single_row = false;
    std::pair<Names, Names> extractCorrelations(Names & correlation);
    std::pair<Names, Names> extractJoinClause(Names & correlation);
    std::vector<ConstASTPtr> extractFilter();
};

/**
 * Split correlation part and un-correlation part in subquery.
 */
class Decorrelation
{
public:
    static std::optional<DecorrelationResult> decorrelateFilters(PlanNodePtr & node, Names & correlation, Context & context);
};

class DecorrelationVisitor : public PlanNodeVisitor<std::optional<DecorrelationResult>, Context>
{
public:
    DecorrelationVisitor(Names & correlation_) : correlation(correlation_) { }
    std::optional<DecorrelationResult> visitPlanNode(PlanNodeBase & node, Context & context) override;
    std::optional<DecorrelationResult> visitFilterNode(FilterNode & node, Context & context) override;
    std::optional<DecorrelationResult> visitProjectionNode(ProjectionNode & node, Context & context) override;

private:
    Names correlation;
};

}
