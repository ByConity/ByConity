#pragma once
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/AggregatingStep.h>

namespace DB
{
struct CostContext;

class AggregatingCost
{
public:
    static PlanNodeCost calculate(const AggregatingStep & step, CostContext & context);
};

}
