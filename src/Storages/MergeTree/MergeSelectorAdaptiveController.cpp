#include <Storages/MergeTree/MergeSelectorAdaptiveController.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// return average of all none zero values
static double getAverageWithoutZero(double a, double b, double abs_zero = 1E-6)
{
    return std::abs(a) < abs_zero ? b : (std::abs(b) < abs_zero ? a : ((a + b) / 2.0));
}

void MergeSelectorAdaptiveController::init(
    MergeTreeBgTaskStatisticsPtr & stats,
    const IMergeSelector::PartsRanges & parts_ranges,
    const std::multimap<String, UInt64> & unselectable_part_rows)
{
    estimators.clear();
    if (!now)
        now = time(nullptr);

    /// Currently only support write amplification control, which is not suitable for bucket table.
    if (is_bucket_table || expected_parts < 1)
        return;
    
    /// Update current_parts, current_rows and smallest_part_rows
    std::unordered_set<String> partition_ids;
    for (const auto & parts_range: parts_ranges)
    {
        if (parts_range.size() == 0)
            continue;
        const String & partition_id = getPartitionID(parts_range.front());
        partition_ids.insert(partition_id);

        UInt64 current_parts = 0;
        UInt64 current_rows = 0;
        UInt64 smallest_part_rows = std::numeric_limits<UInt64>::max();
        for (const auto & part: parts_range)
        {
            current_parts++;
            current_rows += part.rows;
            smallest_part_rows = std::min(smallest_part_rows, part.rows);
        }

        auto res = estimators.emplace(partition_id,
            EstimatorElement{.current_parts = current_parts, .current_rows = current_rows, .smallest_part_rows = smallest_part_rows});
        if (!res.second)
        {
            res.first->second.current_parts += current_parts;
            res.first->second.current_rows += current_rows;
            res.first->second.smallest_part_rows = std::min(res.first->second.smallest_part_rows, smallest_part_rows);
        }
    }

    /// Update current_parts, current_rows and smallest_part_rows by unselectable parts rows
    for (const auto & [p, r]: unselectable_part_rows)
    {
        auto it = estimators.find(p);
        if (it == estimators.end())
            continue;
        
        it->second.current_parts++;
        it->second.current_rows += r;
        it->second.smallest_part_rows = std::min(it->second.smallest_part_rows, r);
    }

    /// Update other infos in estimators
    if (stats && max_parts_to_merge > 1)
    {
        stats->executeWithWriteLock([&, this] (MergeTreeBgTaskStatistics::PartitionStatsMap & partition_stats_map)
        {
            for (const auto & partition_id: partition_ids)
            {
                auto it = partition_stats_map.find(partition_id);
                if (it == partition_stats_map.end())
                    continue;
                
                auto & estimator_elem = estimators[partition_id];

                if (auto last_hour_stats_optional = it->second.last_hour_stats.lastIntervalStats(now))
                    std::tie(estimator_elem.last_hour_inserted_bytes, estimator_elem.last_hour_merged_bytes)
                        = last_hour_stats_optional.value();
                if (auto last_6hour_stats_optional = it->second.last_6hour_stats.lastIntervalStats(now))
                    std::tie(estimator_elem.last_6hour_inserted_bytes, estimator_elem.last_6hour_merged_bytes)
                        = last_6hour_stats_optional.value();

                estimator_elem.inserted_parts = it->second.inserted_parts;
                estimator_elem.merged_parts = it->second.merged_parts;
                estimator_elem.last_insert_time = it->second.last_insert_time;

                UInt64 merged_parts = estimator_elem.merged_parts;
                /// If merged bytes is not too much
                if (estimator_elem.last_hour_merged_bytes + estimator_elem.last_6hour_merged_bytes < 1024 * 1024 || merged_parts < 2)
                    continue;

                double last_hour_wa = estimator_elem.last_hour_merged_bytes / (estimator_elem.last_hour_inserted_bytes + 1E-6);
                double last_6hour_wa = estimator_elem.last_6hour_merged_bytes / (estimator_elem.last_6hour_inserted_bytes + 1E-6);
                double wa = getAverageWithoutZero(last_hour_wa, last_6hour_wa);
                double wa_min = std::log(merged_parts) / std::log(max_parts_to_merge); /// Always select `max_parts_to_merge` smallest parts.
                double wa_max = (merged_parts - 1) / 2.0; /// Always select 2 largest parts.

                if (wa > wa_min)
                    estimator_elem.wa = std::make_tuple(std::clamp(wa, wa_min, wa_max), wa_min, wa_max);
            }
        });
    }
}

bool MergeSelectorAdaptiveController::needControlWriteAmplification(const String & partition_id) const
{
    const auto & estimator_elem = getEstimatorElement(partition_id);

    /// Write amplification may be not precise enough if only a little insertions.
    if (is_bucket_table || expected_parts < 1 || !isRealTimePartition(estimator_elem) || !haveEnoughInfo(estimator_elem))
        return false;
    
    return estimator_elem.current_parts <= 4 * expected_parts && std::get<0>(estimator_elem.wa) > std::get<1>(estimator_elem.wa);
}

/// write_amplification, wa_min, wa_max
std::tuple<double, double, double> MergeSelectorAdaptiveController::getWriteAmplification(const String & partition_id) const
{
    const auto & estimator_elem = getEstimatorElement(partition_id);
    return estimator_elem.wa;
}

/// max_parts, max_rows, 0 means unlimited
std::pair<size_t, size_t> MergeSelectorAdaptiveController::getMaxPartsAndRows(const String & partition_id) const
{
    if (is_bucket_table || expected_parts < 1)
        return std::make_pair(max_parts_to_merge, 0); /// Won't modify merge selector settings

    const auto & estimator_elem = getEstimatorElement(partition_id);
    if (estimator_elem.current_parts <= expected_parts)
        return std::make_pair(1, 1); /// Won't allow any merge

    size_t max_parts = estimator_elem.current_parts - expected_parts + 1;
    size_t max_rows = estimator_elem.current_rows / expected_parts + estimator_elem.smallest_part_rows - 1;

    return std::make_pair(max_parts, max_rows);
}

String MergeSelectorAdaptiveController::getPartitionID(const IMergeSelector::Part & part)
{
    if (unlikely(get_partition_id))
        return get_partition_id(part);
    return part.getDataPartPtr()->info.partition_id;
}

}
