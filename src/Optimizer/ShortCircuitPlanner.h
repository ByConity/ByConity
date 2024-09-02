#pragma once

#include <Interpreters/Context_fwd.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{
class ShortCircuitPlanner
{
public:
    static bool isShortCircuitPlan(QueryPlan & query_plan, ContextPtr context);
    static void addExchangeIfNeeded(QueryPlan & query_plan, ContextMutablePtr context);

private:
    class ShortCircuitPlanVisitor;
};
}
