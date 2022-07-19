#include <Optimizer/CardinalityEstimate/UnionEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr UnionEstimator::estimate(std::vector<PlanNodeStatisticsPtr> & children_stats, const UnionStep & step)
{
    PlanNodeStatisticsPtr output;
    auto & out_to_input = step.getOutToInputs();
    for (size_t i = 0; i < children_stats.size(); i++)
    {
        if (!children_stats.at(i))
        {
            continue;
        }

        PlanNodeStatistics & child_stats = *children_stats.at(i);
        auto child_output_stats = mapToOutput(child_stats, out_to_input, i);
        if (!output)
        {
            output = child_output_stats;
        }
        else
        {
            *output += *child_output_stats;
        }
    }
    return output;
}

PlanNodeStatisticsPtr UnionEstimator::mapToOutput(
    PlanNodeStatistics & child_stats, const std::unordered_map<String, std::vector<String>> & out_to_input, size_t index)
{
    std::unordered_map<String, SymbolStatisticsPtr> output_symbol_statistics;

    for (auto & symbol : out_to_input)
    {
        String output_symbol = symbol.first;
        auto & input_symbols = symbol.second;
        output_symbol_statistics[output_symbol] = child_stats.getSymbolStatistics(input_symbols.at(index))->copy();
    }

    return std::make_shared<PlanNodeStatistics>(child_stats.getRowCount(), output_symbol_statistics);
}

}
