#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
class PlanOptimizer
{
public:
    static void optimize(QueryPlan & plan, ContextMutablePtr context);
    static const Rewriters & getSimpleRewriters();
    static const Rewriters & getFullRewriters();
};

}
