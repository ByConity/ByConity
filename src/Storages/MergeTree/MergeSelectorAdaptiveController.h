#pragma once

#include <common/types.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeSelector.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

namespace DB
{

/// AdaptiveController is a stateless object used to fine tune merge selector settings for each partition by
/// current state and historical statistics. For example, if merged bytes is much greater than inserted bytes,
/// which means write amplification of merge is very high, this information can be used to get a better merge
/// selection result. 
class MergeSelectorAdaptiveController
{
public:
    MergeSelectorAdaptiveController(bool is_bucket_table_, UInt64 expected_parts_, UInt64 max_parts_to_merge_)
        : is_bucket_table(is_bucket_table_)
        , expected_parts(expected_parts_)
        , max_parts_to_merge(max_parts_to_merge_)
    { }
    
    void init(
        MergeTreeBgTaskStatisticsPtr & stats,
        const IMergeSelector::PartsRanges & parts_ranges,
        const std::multimap<String, UInt64> & unselectable_part_rows);

    bool needControlWriteAmplification(const String & partition_id) const;

    /// write_amplification, wa_min, wa_max
    std::tuple<double, double, double> getWriteAmplification(const String & partition_id) const;

    /// max_parts, max_rows, 0 means unlimited
    std::pair<size_t, size_t> getMaxPartsAndRows(const String & partition_id) const;

    /// functions only used in ut
    void debugSetGetPartitionID(std::function<String (const IMergeSelector::Part &)> get_partition_id_) { get_partition_id = get_partition_id_; }
    void debugSetNow(time_t now_) { now = now_; }

protected:
    struct EstimatorElement
    {
        UInt64 last_hour_inserted_bytes{0};
        UInt64 last_6hour_inserted_bytes{0};
        UInt64 last_hour_merged_bytes{0};
        UInt64 last_6hour_merged_bytes{0};
        UInt64 inserted_parts{0};
        UInt64 merged_parts{0};
        UInt64 current_parts{0};
        UInt64 current_rows{0};
        UInt64 smallest_part_rows{0};
        std::tuple<double, double, double> wa{0.0, 0.0, 1.0}; /// write_amplification, wa_min, wa_max
        time_t last_insert_time{0};
    };
    using Estimator = std::unordered_map<String, EstimatorElement>;

    bool isRealTimePartition(const EstimatorElement & estimator_elem) const
    {
        return estimator_elem.inserted_parts > 0 && estimator_elem.last_insert_time + 24 * 60 * 60 > now;
    }

    bool haveEnoughInfo(const EstimatorElement & estimator_elem) const
    {
        return estimator_elem.inserted_parts >= 10;
    }

    String getPartitionID(const IMergeSelector::Part & part);

    const EstimatorElement empty_estimator_elem;
    const EstimatorElement & getEstimatorElement(const String & partition_id) const
    {
        auto it = estimators.find(partition_id);
        if (it == estimators.end())
            return empty_estimator_elem;
        return it->second;
    }

    bool is_bucket_table;
    UInt64 expected_parts;
    UInt64 max_parts_to_merge;
    std::unordered_map<String, EstimatorElement> estimators;
    time_t now{0};

    std::function<String (const IMergeSelector::Part &)> get_partition_id{};
};

using MergeControllerPtr = std::shared_ptr<MergeSelectorAdaptiveController>;

}
