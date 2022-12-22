#pragma once
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <QueryPlan/ExchangeStep.h>

namespace DB
{
struct CostContext;

class ExchangeCost
{
public:
    static PlanNodeCost calculate(const ExchangeStep & node, CostContext & context);
};

}
