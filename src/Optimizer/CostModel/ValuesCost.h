#pragma once
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/ValuesStep.h>

namespace DB
{
struct CostContext;

class ValuesCost
{
public:
    static PlanNodeCost calculate(const ValuesStep & step, CostContext & context);
};

}
