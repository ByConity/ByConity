#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/CostModel/TableScanCost.h>

namespace DB
{

PlanNodeCost TableScanCost::calculate(const TableScanStep &, CostContext & context)
{
    if (!context.stats)
        return PlanNodeCost::ZERO;
    return PlanNodeCost::cpuCost(context.stats->getRowCount()) * context.cost_model.getTableScanCostWeight();
}

}
