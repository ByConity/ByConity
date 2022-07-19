#pragma once
#include <Core/Types.h>

namespace DB
{
class PlanNodeCost
{
public:
    static PlanNodeCost ZERO;

    PlanNodeCost() : PlanNodeCost(0, 0, 0) { }

    PlanNodeCost(double cpu_value_, double net_value_, double mem_value_)
        : cpu_value(cpu_value_), net_value(net_value_), mem_value(mem_value_) { }

    PlanNodeCost(const PlanNodeCost & other) = default;
    PlanNodeCost(PlanNodeCost && other) = default;
    PlanNodeCost & operator=(const PlanNodeCost & other) = default;
    PlanNodeCost & operator=(PlanNodeCost && other) = default;


    static PlanNodeCost cpuCost(double cost) { return PlanNodeCost{cost, 0.0, 0.0}; }
    static PlanNodeCost cpuCost(size_t cost) { return cpuCost(static_cast<double>(cost)); }
    static PlanNodeCost netCost(double cost) { return PlanNodeCost{0.0, cost, 0.0}; }
    static PlanNodeCost netCost(size_t cost) { return netCost(static_cast<double>(cost)); }
    static PlanNodeCost memCost(double cost) { return PlanNodeCost{0.0, 0.0, cost}; }
    static PlanNodeCost memCost(size_t cost) { return memCost(static_cast<double>(cost)); }

    double getCost() const;

    PlanNodeCost & operator+(const PlanNodeCost & other)
    {
        cpu_value = cpu_value + other.cpu_value;
        net_value = net_value + other.net_value;
        mem_value = mem_value + other.mem_value;
        return *this;
    }

    PlanNodeCost & operator*(size_t multiply) { return operator*(static_cast<double>(multiply)); }
    PlanNodeCost & operator*(double multiply)
    {
        cpu_value = cpu_value * multiply;
        net_value = net_value * multiply;
        mem_value = mem_value * multiply;
        return *this;
    }

private:
    double cpu_value;
    double net_value;
    double mem_value;
};

}
