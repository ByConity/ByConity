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

#include <Optimizer/Property/PropertyMatcher.h>

#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Optimizer/Property/Property.h>

#include <algorithm>
#include <optional>
#include <vector>

namespace DB
{
bool PropertyMatcher::matchNodePartitioning(
    const Context & context,
    Partitioning & required,
    const Partitioning & actual,
    const SymbolEquivalences & equivalences,
    const Constants & constants)
{
    if (required.getHandle() == Partitioning::Handle::ARBITRARY)
        return true;

    if (required.getHandle() == Partitioning::Handle::FIXED_HASH && context.getSettingsRef().enforce_round_robin
        && required.isEnforceRoundRobin() && actual.normalize(equivalences).satisfy(required.normalize(equivalences), constants))
    {
        required.setHandle(Partitioning::Handle::FIXED_ARBITRARY);
        return false;
    }

    return actual.normalize(equivalences).satisfy(required.normalize(equivalences), constants);
}

bool PropertyMatcher::matchStreamPartitioning(
    const Context &,
    const Partitioning & required,
    const Partitioning & actual,
    const SymbolEquivalences & equivalences,
    const Constants & constants,
    bool match_local_exchange)
{
    if (required.getHandle() == Partitioning::Handle::ARBITRARY)
        return true;
    // todo remove
    if (!match_local_exchange)
    {
        if (required.getHandle() == Partitioning::Handle::FIXED_HASH)
            return true;
        if (required.getHandle() == Partitioning::Handle::FIXED_ARBITRARY)
            return true;
    }

    return actual.normalize(equivalences).satisfy(required.normalize(equivalences), constants);
}

Sorting PropertyMatcher::matchSorting(
    const Context & context, const Sorting & required, const Sorting & actual, const SymbolEquivalences & equivalences, const Constants & constants)
{
    return matchSorting(context, required.toSortDesc(), actual, equivalences, constants);
}

/// Optimize in case of exact match with order key element
/// or in some simple cases when order key element is wrapped into monotonic function.
/// Returns on of {-1, 0, 1} - direction of the match. 0 means - doesn't match.
SortOrder matchSortDescription(const SortColumnDescription & require, const SortColumnDescription & actual)
{
    /// If required order depend on collation, it cannot be matched with primary key order.
    /// Because primary keys cannot have collations.
    if (require.collator)
        return SortOrder::UNKNOWN;

    if (actual.collator)
        return SortOrder::UNKNOWN;

    auto match_direction = [&](int require_dir, int actual_dir) {
        int current_direction = 0;
        switch (require_dir)
        {
            case 1: {
                if (actual_dir == 0 || actual_dir == 1)
                {
                    current_direction = 1;
                }
                break;
            }
            case -1: {
                if (actual_dir == 0 || actual_dir == -1)
                {
                    current_direction = -1;
                }
                break;
            }
            case 0: {
                if (actual_dir != 0)
                {
                    current_direction = actual_dir;
                }
                else
                {
                    current_direction = 1;
                }
            }
        }
        return current_direction;
    };

    int direction = match_direction(require.direction, actual.direction);
    int null_direction = match_direction(require.nulls_direction, actual.nulls_direction);


    /// For the path: order by (sort_column, ...)
    if (require.column_name == actual.column_name)
        return SortColumn::directionToSortOrder(direction, null_direction);

    return SortOrder::UNKNOWN;
}

Sorting PropertyMatcher::matchSorting(const Context &, const SortDescription & required, const Sorting & actual, const SymbolEquivalences &, const Constants & constants)
{
    if (!actual.empty())
    {
        SortDescription sort_description_for_merging;
        sort_description_for_merging.reserve(required.size());

        size_t desc_pos = 0;
        size_t key_pos = 0;

        while (desc_pos < required.size() && key_pos < actual.size())
        {
            auto match = matchSortDescription(required[desc_pos], actual[key_pos].toSortColumnDesc());
            bool is_matched = match != SortOrder::UNKNOWN;
            if (!is_matched)
            {
                /// If one of the sorting columns is constant after filtering,
                /// skip it, because it won't affect order anymore.
                if (constants.contains(actual[key_pos].getName()))
                {
                    ++key_pos;
                    continue;
                }
                else if (constants.contains(required[desc_pos].column_name))
                {
                    sort_description_for_merging.push_back(required[desc_pos]);

                    ++desc_pos;
                    continue;
                }
                break;
            }

            sort_description_for_merging.push_back(required[desc_pos]);

            ++desc_pos;
            ++key_pos;
        }

        return Sorting{sort_description_for_merging};
    }
    return {};
}

template <typename T>
static std::vector<T> intersection(const std::vector<std::vector<T>> & inputs)
{
    if (inputs.empty())
        return {};
    if (inputs.size() == 1)
        return inputs[0];

    std::unordered_set<std::string> common_elements;
    for (const auto & element : inputs[0])
        common_elements.emplace(element);

    for (size_t i = 1; i < inputs.size(); i++)
    {
        std::unordered_set<std::string> new_common_elements;
        for (const auto & element : inputs[i])
            if (common_elements.contains(element))
                new_common_elements.emplace(element);
        common_elements = std::move(new_common_elements);
    }

    std::vector<std::string> ordered_common_elements;
    for (const auto & element : inputs[0])
        if (common_elements.contains(element))
            ordered_common_elements.emplace_back(element);

    return ordered_common_elements;
}

Property PropertyMatcher::compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & required_properties)
{
    if (required_properties.empty())
        return Property{};

    // check if all requries are broadcast
    bool all_broadcast = std::all_of(required_properties.begin(), required_properties.end(), [](const auto & property) {
        return property.getNodePartitioning().getHandle() == Partitioning::Handle::FIXED_BROADCAST;
    });
    if (all_broadcast)
    {
                Property common_property{Partitioning{Partitioning::Handle::FIXED_BROADCAST}};
                return common_property;
    }

    // find common partition_handle && partition_columns ignore ARBITRARY / FIXED_ARBITRARY / FIXED_BROADCAST
    bool has_partition_columns = false;
    bool has_required_handle = false;
    std::vector<Partitioning::Handle> partition_handles;
    std::vector<std::vector<String>> partition_columns;
    for (const auto & property : required_properties)
    {
        auto partition_handle = property.getNodePartitioning().getHandle();
// don't support single / coordinator right now, as the cost of them are incorrect, caused cte to choose the wrong plan
        if (partition_handle == Partitioning::Handle::ARBITRARY || partition_handle == Partitioning::Handle::FIXED_ARBITRARY
            || partition_handle == Partitioning::Handle::FIXED_BROADCAST || partition_handle == Partitioning::Handle::SINGLE
            || partition_handle == Partitioning::Handle::COORDINATOR)
            continue;

        partition_handles.emplace_back(partition_handle);
                has_partition_columns |= !property.getNodePartitioning().getColumns().empty();
has_required_handle |= property.getNodePartitioning().isRequireHandle();
        partition_columns.emplace_back(property.getNodePartitioning().getColumns());
    }

    if (partition_handles.empty())
        return Property{};
    Partitioning::Handle common_partition_handle = partition_handles[0];
    for (size_t i = 1; i < partition_handles.size(); i++)
        if (partition_handles[i] != common_partition_handle)
            return Property{};

    // find common prefix partition columns
    std::vector<String> common_partition_columns = intersection(partition_columns);
    if (has_partition_columns && common_partition_columns.empty())
        return Property{};


    Property common_property{Partitioning{common_partition_handle, common_partition_columns}};

    if (has_required_handle)
    common_property.getNodePartitioningRef().setRequireHandle(true);

    return common_property;
}
}
