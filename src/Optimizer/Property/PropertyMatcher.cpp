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
    const Context & context, Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences, const Constants & constants)
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
std::optional<SortOrder> matchSortDescription(const SortColumnDescription & require, const SortColumnDescription & actual)
{
    /// If required order depend on collation, it cannot be matched with primary key order.
    /// Because primary keys cannot have collations.
    if (require.collator)
        return {};

    if (actual.collator)
        return {};

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

    return {};
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
            bool is_matched = match && (desc_pos == 0 || match == read_direction);

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
                read_direction = match.value();

            sort_description_for_merging.push_back(required[desc_pos]);

            ++desc_pos;
            ++key_pos;
        }

        return Sorting{sort_description_for_merging};
    }
    return {};
}


Property PropertyMatcher::compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & required_properties)
{
    if (required_properties.empty())
        return Property{};
    if (required_properties.size() == 1)
        return Property{required_properties.begin()->getNodePartitioning()};

    Property res;
    bool is_all_broadcast = !required_properties.empty();
    auto it = required_properties.begin();
    for (; it != required_properties.end(); ++it)
    {
        auto partition_handle = it->getNodePartitioning().getPartitioningHandle();
        is_all_broadcast &= partition_handle == Partitioning::Handle::FIXED_BROADCAST;
        if (partition_handle == Partitioning::Handle::ARBITRARY || partition_handle == Partitioning::Handle::FIXED_ARBITRARY
            || partition_handle == Partitioning::Handle::FIXED_BROADCAST)
            continue;
        res = *it;
        break;
    }

    const auto & node_partition = res.getNodePartitioning();
    const auto handle = node_partition.getPartitioningHandle();
    std::unordered_set<String> columns_set;
    for (const auto & item : node_partition.getPartitioningColumns())
        columns_set.emplace(item);

    for (; it != required_properties.end(); ++it)
    {
        auto partition_handle = it->getNodePartitioning().getPartitioningHandle();
        is_all_broadcast &= partition_handle == Partitioning::Handle::FIXED_BROADCAST;
        if (partition_handle == Partitioning::Handle::ARBITRARY || partition_handle == Partitioning::Handle::FIXED_ARBITRARY
            || partition_handle == Partitioning::Handle::FIXED_BROADCAST)
            continue;

        if (partition_handle != handle)
            return Property{};

        if (partition_handle == Partitioning::Handle::FIXED_HASH)
        {
            std::unordered_set<String> intersection;
            const auto & partition_columns = it->getNodePartitioning().getPartitioningColumns();
            std::copy_if(
                partition_columns.begin(),
                partition_columns.end(),
                std::inserter(intersection, intersection.begin()),
                [&columns_set](const auto & e) { return columns_set.contains(e); });
            if (intersection.empty())
                return Property{}; // no common property
            columns_set.swap(intersection);
        }
    }

    if (is_all_broadcast)
        return Property{Partitioning{Partitioning::Handle::FIXED_BROADCAST}};

    Names partition_columns{columns_set.begin(), columns_set.end()};

    // no need to consider require_handle / buckets / enforce_round_robin for required property
    return Property{Partitioning{handle, std::move(partition_columns)}};
}
}
