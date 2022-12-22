#pragma once

#include <Optimizer/CostModel/PlanNodeCost.h>

namespace DB
{
struct CostContext;

class TableScanCost
{
public:
    static PlanNodeCost calculate(const TableScanStep & step, CostContext & context);
};

}
