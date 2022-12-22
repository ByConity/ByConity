#include <fstream>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Interpreters/InterpreterDumpInfoQueryUseOptimizer.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Dump/PlanDump.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PlanOptimizer.h>
#include <Parsers/ASTDumpInfoQuery.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/QueryPlanner.h>

namespace DB
{
BlockIO InterpreterDumpInfoQueryUseOptimizer::execute()
{
    auto query_body_ptr = query_ptr->clone();
    context->createPlanNodeIdAllocator();
    context->createSymbolAllocator();
    context->createOptimizerMetrics();
    BlockIO res;
    bool verbose = true;

    ASTPtr query_body = query_body_ptr->as<ASTDumpInfoQuery>()->dump_query;
    dumpQuery(query_body_ptr->as<ASTDumpInfoQuery>()->dump_string, context);

    query_body = QueryRewriter::rewrite(query_body, context);
    AnalysisPtr analysis = QueryAnalyzer::analyze(query_body, context);
    QueryPlanPtr query_plan = QueryPlanner::plan(query_body, *analysis, context);
    dumpDdlStats(*query_plan, context);
    dumpClusterInfo(context, WorkerSizeFinder::find(*query_plan, *context));

    PlanOptimizer::optimize(*query_plan, context);
    CardinalityEstimator::estimate(*query_plan, context);
    std::unordered_map<PlanNodeId, double> costs = CostCalculator::calculate(*query_plan, *context);
    String explain = PlanPrinter::textLogicalPlan(*query_plan, context, true, verbose, costs);

    String path = context->getSettingsRef().graphviz_path.toString() + context->getCurrentQueryId() + "/explain.txt";
    std::ofstream out(path);
    out << explain;
    out.close();

    String path_dir = context->getSettingsRef().graphviz_path.toString();
    String src_dir = path_dir + context->getCurrentQueryId() + "/";
    String des_file = path_dir + context->getCurrentQueryId() + ".zip";
    zipDirectory(des_file, src_dir);

    return res;
}
}
