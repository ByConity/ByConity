#pragma once
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
class ProjectionEstimator
{
public:
    static PlanNodeStatisticsPtr estimate(PlanNodeStatisticsPtr & child_stats, const ProjectionStep & step);
};

// calculate expression stats
class ScalarStatsCalculator : public ConstASTVisitor<SymbolStatisticsPtr, std::unordered_map<String, SymbolStatisticsPtr>>
{
public:
    static SymbolStatisticsPtr
    estimate(ConstASTPtr expression, const DataTypePtr & type_, UInt64 total_rows_, std::unordered_map<String, SymbolStatisticsPtr> SymbolStatisticsPtr);
    SymbolStatisticsPtr visitNode(const ConstASTPtr & node, std::unordered_map<String, SymbolStatisticsPtr> & context) override;
    SymbolStatisticsPtr visitASTIdentifier(const ConstASTPtr & node, std::unordered_map<String, SymbolStatisticsPtr> & context) override;
    SymbolStatisticsPtr visitASTFunction(const ConstASTPtr & node, std::unordered_map<String, SymbolStatisticsPtr> & context) override;
    SymbolStatisticsPtr visitASTLiteral(const ConstASTPtr & node, std::unordered_map<String, SymbolStatisticsPtr> & context) override;

private:
    ScalarStatsCalculator(const DataTypePtr & type_, UInt64 total_rows_) : type(type_), total_rows(total_rows_) { }
    const DataTypePtr & type;
    UInt64 total_rows;
};

}
