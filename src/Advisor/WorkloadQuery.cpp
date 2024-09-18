#include <Advisor/WorkloadQuery.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Core/QualifiedTableName.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/PlanOptimizer.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Parsers/ParserQuery.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanPattern.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/QueryPlanner.h>

#include <algorithm>
#include <set>
#include <utility>

namespace DB
{
static std::pair<Rewriters, Rewriters> getRewriters()
{
    auto full_rewriters = PlanOptimizer::getFullRewriters();
    auto it = std::find_if(
        full_rewriters.begin(), full_rewriters.end(), [](const auto & rewriter) { return rewriter->name() == "CascadesOptimizer"; });
    Rewriters before_cascades(full_rewriters.begin(), it);
    Rewriters after_cascades(it, full_rewriters.end());
    return std::make_pair(before_cascades, after_cascades);
}

namespace
{
    // CostCalculator calculates cost only if all stats can be derived
    // here we calculate cost as much as possible
    PlanCostMap calculateCost(QueryPlan & plan, const Context & context)
    {
        PlanCostMap plan_cost_map;
        size_t worker_size = WorkerSizeFinder::find(plan, context);
        auto cte_ref_counts = plan.getCTEInfo().collectCTEReferenceCounts(plan.getPlanNode());
        PlanCostVisitor visitor{CostModel{context}, worker_size, plan.getCTEInfo(), cte_ref_counts};
        VisitorUtil::accept(plan.getPlanNode(), visitor, plan_cost_map);
        return plan_cost_map;
    }
}

class TableFinder : public PlanNodeVisitor<Void, Void>
{
public:
    static std::set<QualifiedTableName> find(QueryPlan & plan)
    {
        TableFinder visitor{plan.getCTEInfo()};
        Void c{};
        VisitorUtil::accept(plan.getPlanNode(), visitor, c);
        return visitor.tables;
    }
protected:
    Void visitTableScanNode(TableScanNode & node, Void &) override
    {
        const TableScanStep * table_step = dynamic_cast<const TableScanStep *>(node.getStep().get());
        const StorageID storage_id = table_step->getStorage()->getStorageID();
        tables.emplace(QualifiedTableName{storage_id.database_name, storage_id.table_name});
        return Void{};
    }
    Void visitPlanNode(PlanNodeBase & node, Void & context) override
    {
        for (auto & child : node.getChildren())
            VisitorUtil::accept(child, *this, context);
        return Void{};
    }
    Void visitCTERefNode(CTERefNode & node, Void & context) override
    {
        const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
        cte_helper.accept(cte->getId(), *this, context);
        return Void{};
    }
private:
    explicit TableFinder(CTEInfo & cte_info_) : cte_helper(cte_info_) { }
    std::set<QualifiedTableName> tables;
    SimpleCTEVisitHelper<void> cte_helper;
};

WorkloadQueryPtr WorkloadQuery::build(const std::string & query_id, const std::string & query, const ContextPtr & from_context)
{
    ContextMutablePtr context = Context::createCopy(from_context);
    context->applySettingsChanges(
        {DB::SettingChange("enable_sharding_optimize", "true"), // for colocated join
         DB::SettingChange("enable_runtime_filter", "false"), // for calculating signature
         DB::SettingChange("enable_optimzier", "true"),
         DB::SettingChange("cte_mode", "INLINED")}); // for materialized view
    context->createPlanNodeIdAllocator();
    context->createSymbolAllocator();
    context->createOptimizerMetrics();
    context->makeQueryContext();

    if (context->getSettingsRef().print_graphviz)
    {
        std::stringstream path;
        path << context->getSettingsRef().graphviz_path.toString();
        path << "/" << query_id << ".sql";
        std::ofstream out(path.str());
        out << query;
        out.close();
    }

    // parse and plan
    const char * begin = query.data();
    const char * end = begin + query.size();
    ParserQuery parser(end, ParserSettings::valueOf(context->getSettingsRef()));
    auto ast = parseQuery(parser, begin, end, "", context->getSettingsRef().max_query_size, context->getSettingsRef().max_parser_depth);
    ast = QueryRewriter().rewrite(ast, context);
    AnalysisPtr analysis = QueryAnalyzer::analyze(ast, context);
    QueryPlanPtr query_plan = QueryPlanner().plan(ast, *analysis, context);

    static auto [before_cascades, after_cascades] = getRewriters();

    // before cascades
    PlanOptimizer::optimize(*query_plan, context, before_cascades);
    auto plan_before_cascades = query_plan->copy(context);

    // complete optimization
    PlanOptimizer::optimize(*query_plan, context, after_cascades);

    std::set<QualifiedTableName> query_tables = TableFinder::find(*query_plan);

    CardinalityEstimator::estimate(*query_plan, context);
    PlanCostMap costs = calculateCost(*query_plan, *context);
    
    return std::make_unique<WorkloadQuery>(
        context, query_id, query, std::move(query_plan), std::move(plan_before_cascades), std::move(query_tables), std::move(costs));
}

WorkloadQueries WorkloadQuery::build(const std::vector<std::string> & queries, const ContextPtr & from_context, ThreadPool & query_thread_pool)
{
    WorkloadQueries res(queries.size());
    auto thread_group = CurrentThread::getGroup();
    for (size_t i = 0; i < queries.size(); ++i)
    {
        query_thread_pool.scheduleOrThrowOnError([&, i] {
            setThreadName("BuildQuery");
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            LOG_DEBUG(getLogger("WorkloadQuery"), "start building query {}", i);
            const auto & query = queries[i];
            try
            {
                WorkloadQueryPtr workload_query = build("q" + std::to_string(i), query, from_context);
                res[i] = std::move(workload_query);
            } catch (...)
            {
                LOG_WARNING(getLogger("WorkloadQuery"),"failed to build query, reason: {}, sql: {}",
                    getCurrentExceptionMessage(true), query);
            }
        });
    }
    query_thread_pool.wait();
    res.erase(std::remove(res.begin(), res.end(), nullptr), res.end());
    LOG_DEBUG(getLogger("WorkloadQuery"), "built queries {}/{}", res.size(), queries.size());
    return res;
}

double WorkloadQuery::getOptimalCost(const TableLayout & table_layout)
{
    if (!root_group)
    {
        cascades_context = std::make_shared<CascadesContext>(
            query_context,
            plan_before_cascades->getCTEInfo(),
            WorkerSizeFinder::find(*plan_before_cascades, *query_context),
            PlanPattern::maxJoinSize(*plan_before_cascades, query_context),
            true);
        root_group = cascades_context->initMemo(plan_before_cascades->getPlanNode());
    }

    TableLayout relevant_table_layout;

    for (const auto & entry : table_layout)
    {
        if (query_tables.contains(entry.first))
            relevant_table_layout.emplace(entry.first, entry.second);
    }

    Property required_property{Partitioning{Partitioning::Handle::SINGLE}};
    required_property.setTableLayout(std::move(relevant_table_layout));
    GroupId root_group_id = root_group->getGroupId();
    CascadesOptimizer::optimize(root_group_id, *cascades_context, required_property);
    auto res = cascades_context->getMemo().getGroupById(root_group_id)->getBestExpression(required_property)->getCost();

    GraphvizPrinter::printMemo(cascades_context->getMemo(), root_group_id, query_context, "CascadesOptimizer-Memo-Graph");
    return res;
}

}
