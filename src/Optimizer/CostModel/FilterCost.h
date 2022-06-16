#pragma once
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/FilterStep.h>

namespace DB
{
struct CostContext;

class FilterCost
{
public:
    static PlanNodeCost calculate(const FilterStep & step, CostContext & context);
};

}
