#pragma once

#include <Interpreters/Context.h>

namespace DB
{
class CostModel
{
public:
    static constexpr double CPU_COST_RATIO = 0.75;
    static constexpr double NET_COST_RATIO = 0.15;
    static constexpr double MEM_COST_RATIO = 0.1;

    explicit CostModel(const Context & context) : context_settings(context.getSettingsRef()) { }

    double getAggregateCostWeight() const { return context_settings.cost_calculator_aggregating_weight; }

    double getJoinProbeSideCostWeight() const { return context_settings.cost_calculator_join_probe_weight; }
    double getJoinBuildSideCostWeight() const { return context_settings.cost_calculator_join_build_weight; }
    double getJoinOutputCostWeight() const { return context_settings.cost_calculator_join_output_weight; }

    double getReadFromStorageCostWeight() const { return context_settings.cost_calculator_table_scan_weight; }

    double getCTECostWeight() const { return context_settings.cost_calculator_cte_weight; }

    double getProjectionCostWeight() const { return context_settings.cost_calculator_projection_weight; }

private:
    const Settings & context_settings;
};

}
