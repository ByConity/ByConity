#pragma once
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>


namespace DB
{
class PlanCheck
{
public:
    static void checkInitPlan(QueryPlan & plan, ContextMutablePtr context);
    static void checkFinalPlan(QueryPlan & plan, ContextMutablePtr context);
};

class ReadNothingChecker : public PlanNodeVisitor<Void, Void>
{
public:
    static void check(PlanNodePtr plan);

    Void visitPlanNode(PlanNodeBase &, Void &) override;
    Void visitReadNothingNode(ReadNothingNode &, Void &) override;
};

class SymbolChecker : public PlanNodeVisitor<Void, ContextMutablePtr>
{
public:
    static void check(QueryPlan & plan, ContextMutablePtr & context, bool check_filter);

    SymbolChecker(bool checkFilter) : check_filter(checkFilter) { }

    Void visitPlanNode(PlanNodeBase &, ContextMutablePtr &) override;
    Void visitProjectionNode(ProjectionNode &, ContextMutablePtr &) override;
    Void visitFilterNode(FilterNode &, ContextMutablePtr &) override;

private:
    bool check_filter;
};

}
