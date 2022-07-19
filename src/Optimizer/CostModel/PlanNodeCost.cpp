#include <Optimizer/CostModel/CostModel.h>
#include <Optimizer/CostModel/PlanNodeCost.h>

namespace DB
{
PlanNodeCost PlanNodeCost::ZERO(0.0, 0.0, 0.0);

double PlanNodeCost::getCost() const
{
    return cpu_value * CostModel::CPU_COST_RATIO + mem_value * CostModel::MEM_COST_RATIO + net_value * CostModel::NET_COST_RATIO;
}

}
