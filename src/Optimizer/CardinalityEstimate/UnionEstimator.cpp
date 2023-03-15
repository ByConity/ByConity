/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/CardinalityEstimate/UnionEstimator.h>

namespace DB
{
PlanNodeStatisticsPtr UnionEstimator::estimate(std::vector<PlanNodeStatisticsPtr> & children_stats, const UnionStep & step)
{
    PlanNodeStatisticsPtr output;
    const auto & out_to_input = step.getOutToInputs();
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

    for (const auto & [k, v] : out_to_input)
    {
        output_symbol_statistics.insert_or_assign(k, child_stats.getSymbolStatistics(v.at(index))->copy());
    }

    return std::make_shared<PlanNodeStatistics>(child_stats.getRowCount(), std::move(output_symbol_statistics));
}

}
