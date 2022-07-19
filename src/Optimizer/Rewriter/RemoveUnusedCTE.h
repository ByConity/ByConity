#pragma once

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTERefStep.h>

namespace DB
{
/**
 * 1. Always Inlining Single-use CTEs
 * 2. Remove unused CTE from cte_info
 */
class RemoveUnusedCTE : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "RemoveUnusedCTE"; }

private:
    class Rewriter;
};
}
