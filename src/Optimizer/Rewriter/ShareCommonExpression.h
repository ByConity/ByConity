#pragma once
#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>

namespace DB
{

class ShareCommonExpression : public Rewriter
{
public:
    String name() const override
    {
        return "ShareCommonExpression";
    }

private:
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_common_expression_sharing;
    }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    static PlanNodePtr rewriteImpl(PlanNodePtr root, ContextMutablePtr context);
};
}
