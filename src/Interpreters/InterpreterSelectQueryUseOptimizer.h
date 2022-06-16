#pragma once
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <QueryPlan/PlanVisitor.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/QueryLog.h>

namespace DB
{
class InterpreterSelectQueryUseOptimizer : public IInterpreter
{
public:
    InterpreterSelectQueryUseOptimizer(const ASTPtr & query_ptr_, ContextMutablePtr & context_, const SelectQueryOptions & options_)
        : query_ptr(query_ptr_), context(context_), options(options_)
    {
    }

    QueryPlanPtr buildQueryPlan();

    BlockIO execute() override;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override
    {
        elem.query_kind = "Select";
    }

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    SelectQueryOptions options;
};

/**
 * Convert PlanNode to QueryPlan::Node.
 */
class PlanNodeToNodeVisitor : public PlanNodeVisitor<QueryPlan::Node *, Void>
{
public:
    static QueryPlan convert(QueryPlan &);
    explicit PlanNodeToNodeVisitor(QueryPlan & plan_) : plan(plan_) { }
    QueryPlan::Node * visitPlanNode(PlanNodeBase & node, Void & c) override;

private:
    QueryPlan & plan;
};

struct ClusterInfoContext
{
    QueryPlan & query_plan;
    ContextMutablePtr context;
    PlanSegmentTreePtr & plan_segment_tree;
};

class ClusterInfoFinder : public PlanNodeVisitor<std::optional<PlanSegmentContext>, ClusterInfoContext>
{
public:
    static PlanSegmentContext find(PlanNodePtr & node, ClusterInfoContext & cluster_info_context);
    std::optional<PlanSegmentContext> visitPlanNode(PlanNodeBase & node, ClusterInfoContext & cluster_info_context) override;
    std::optional<PlanSegmentContext> visitTableScanNode(TableScanNode & node, ClusterInfoContext & cluster_info_context) override;
};
}
