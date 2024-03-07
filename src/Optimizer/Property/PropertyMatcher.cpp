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

#include <optional>
#include <Optimizer/Property/PropertyMatcher.h>

#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Optimizer/Property/Property.h>

namespace DB
{
bool PropertyMatcher::matchNodePartitioning(
    const Context & context,
    Partitioning & required,
    const Partitioning & actual,
    const SymbolEquivalences & equivalences,
    const Constants & constants)
{
    if (required.getPartitioningHandle() == Partitioning::Handle::ARBITRARY)
        return true;

    if (required.getPartitioningHandle() == Partitioning::Handle::FIXED_HASH && context.getSettingsRef().enforce_round_robin
        && required.isEnforceRoundRobin() && actual.normalize(equivalences).satisfy(required.normalize(equivalences), constants))
    {
        required.setHandle(Partitioning::Handle::FIXED_ARBITRARY);
        return false;
    }

    return actual.normalize(equivalences).satisfy(required.normalize(equivalences), constants);
}

bool PropertyMatcher::matchStreamPartitioning(
    const Context &, const Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences)
{
    if (required.getPartitioningHandle() == Partitioning::Handle::ARBITRARY)
        return true;
    return required.normalize(equivalences) == actual.normalize(equivalences);
}


Sorting PropertyMatcher::matchSorting(
    const Context & context, const Sorting & required, const Sorting & actual, const SymbolEquivalences & equivalences)
{
    return matchSorting(context, required.toSortDesc(), actual, equivalences);
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

Sorting PropertyMatcher::matchSorting(const Context &, const SortDescription & required, const Sorting & actual, const SymbolEquivalences &)
{
    if (!actual.empty())
    {
        SortOrder read_direction = SortOrder::UNKNOWN;

        // todo@jingpeng.mt constant
        // auto fixed_sorting_columns = getFixedSortingColumns(query, sorting_key_columns, context);

        SortDescription sort_description_for_merging;
        sort_description_for_merging.reserve(required.size());

        size_t desc_pos = 0;
        size_t key_pos = 0;

        while (desc_pos < required.size() && key_pos < actual.size())
        {
            auto match = matchSortDescription(required[desc_pos], actual[key_pos].toSortColumnDesc());
            bool is_matched = match != SortOrder::UNKNOWN && (desc_pos == 0 || match == read_direction);

            if (!is_matched)
            {
                /// If one of the sorting columns is constant after filtering,
                /// skip it, because it won't affect order anymore.
                // if (fixed_sorting_columns.contains(sorting_key_columns[key_pos]))
                // {
                //     ++key_pos;
                //     continue;
                // }

                break;
            }

            if (desc_pos == 0)
                read_direction = match;

            sort_description_for_merging.push_back(required[desc_pos]);

            ++desc_pos;
            ++key_pos;
        }

        return Sorting{sort_description_for_merging};
    }
    return {};
}


template <typename T>
static std::vector<T> findCommonPrefix(const std::vector<std::vector<T>> & input)
{
    if (input.empty())
        return {};

    std::vector<T> prefix = input[0];
    for (size_t i = 1; i < input.size(); i++)
    {
        const auto & current = input[i];

        size_t j = 0;
        while (j < prefix.size() && j < current.size() && prefix[j] == current[j])
            j++;

        prefix.resize(j);
    }

    return prefix;
}

Property PropertyMatcher::compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & required_properties)
{
    if (required_properties.empty())
        return Property{};
    if (required_properties.size() == 1)
        return *required_properties.begin();

    // check if all requries are broadcast
    bool all_broadcast = std::all_of(required_properties.begin(), required_properties.end(), [](const auto & property) {
        return property.getNodePartitioning().getPartitioningHandle() == Partitioning::Handle::FIXED_BROADCAST;
    });
    if (all_broadcast)
    {
        bool all_preferred = std::all_of(
            required_properties.begin(), required_properties.end(), [](const auto & property) { return property.isPreferred(); });
        Property common_property{Partitioning{Partitioning::Handle::FIXED_BROADCAST}};
        common_property.setPreferred(all_preferred);
        return common_property;
    }

    // find common partition_handle && partition_columns ignore ARBITRARY / FIXED_ARBITRARY / FIXED_BROADCAST
    bool all_preferred = true;
    bool has_partition_columns = false;
    std::vector<Partitioning::Handle> partition_handles;
    std::vector<std::vector<String>> partition_columns;
    for (const auto & property : required_properties)
    {
        auto partition_handle = property.getNodePartitioning().getPartitioningHandle();
        if (partition_handle == Partitioning::Handle::ARBITRARY || partition_handle == Partitioning::Handle::FIXED_ARBITRARY
            || partition_handle == Partitioning::Handle::FIXED_BROADCAST)
            continue;

        partition_handles.emplace_back(partition_handle);
        all_preferred &= property.isPreferred();
        has_partition_columns |= !property.getNodePartitioning().getPartitioningColumns().empty();
        partition_columns.emplace_back(property.getNodePartitioning().getPartitioningColumns());
    }

    if (partition_handles.empty())
        return Property{};
    Partitioning::Handle common_partition_handle = partition_handles[0];
    for (size_t i = 1; i < partition_handles.size(); i++)
        if (partition_handles[i] != common_partition_handle)
            return Property{};

    // find common prefix partition columns
    std::vector<String> common_prefix_partition_columns = findCommonPrefix(partition_columns);
    if (has_partition_columns && common_prefix_partition_columns.empty())
        return Property{};

    Property common_property{Partitioning{common_partition_handle, common_prefix_partition_columns}};
    common_property.setPreferred(all_preferred);
    return common_property;
}
}
