#pragma once
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanVisitor.h>

namespace DB
{
class BitmapIndexSplitter : public Rewriter
{
public:
    String name() const override { return "BitmapIndexSplitter"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_bitmap_index_splitter; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
};

struct SplitterContext
{
    Assignments assignments;
    EqualityASTMap<String> ast_to_name;
};

class FuncSplitter : public PlanNodeVisitor<PlanNodePtr, SplitterContext>
{
public:
    explicit FuncSplitter(ContextMutablePtr context_) : context(context_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase & node, SplitterContext & splitter_context) override;
    PlanNodePtr visitFilterNode(FilterNode & node, SplitterContext & context) override;
    PlanNodePtr visitProjectionNode(ProjectionNode & node, SplitterContext & context) override;

private:
    ContextMutablePtr context;
};


class CollectFuncs : public ConstASTVisitor<ConstASTPtr, Assignments>
{
public:
    explicit CollectFuncs(const NameToType & name_to_type_, ContextMutablePtr & context_) : name_to_type(name_to_type_), context(context_) { }

    ConstASTPtr visitASTFunction(const ConstASTPtr & node, Assignments & assignments) override;
    ConstASTPtr visitNode(const ConstASTPtr & node, Assignments & c) override
    {
        for (const ASTPtr & child : node->children)
        {
            ASTVisitorUtil::accept(child, *this, c);
        }
        return node;
    }

    static Assignments collect(ConstASTPtr ast, const NameToType & name_to_type, ContextMutablePtr context)
    {
        CollectFuncs rewriter{name_to_type, context};
        Assignments assignments;
        ASTVisitorUtil::accept(ast, rewriter, assignments);
        return assignments;
    }

private:
    const NameToType & name_to_type;
    ContextMutablePtr context;
};


}
