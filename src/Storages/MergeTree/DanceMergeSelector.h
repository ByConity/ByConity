#pragma once

#include <Core/BaseSettings.h>
#include <Storages/MergeTree/MergeSelector.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
class MergeTreeData;

#define LIST_OF_DANCE_MERGE_SELECTOR_SETTINGS(M) \
    M(UInt64, max_parts_to_merge_base, 100, "", 0) \
    M(UInt64, min_parts_to_merge_base, 5, "", 0) \
\
    M(UInt64, size_fixed_cost_to_add, 5 * 1024 * 1024, "", 0) \
    M(Float, score_count_exp, 1.15, "", 0) \
\
    M(Bool, enable_heuristic_to_align_parts, true, "", 0) \
    M(Float, heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part, 0.9, "", 0) \
    M(Float, heuristic_to_align_parts_max_absolute_difference_in_powers_of_two, 0.5, "", 0) \
    M(Float, heuristic_to_align_parts_max_score_adjustment, 0.75, "", 0) \
\
    M(Bool, enable_penalty_for_small_parts_at_right, true, "", 0) \
    M(Float, enable_penalty_for_small_parts_at_right_ratio_base, 0.1, "", 0) \
    M(Float, score_penalty_for_small_parts_at_right, 1.15, "", 0) \
\
    M(UInt64, min_parts_to_enable_multi_selection, 200, "", 0) \
    /** Unique table will set it to a value <= 2^32 in order to prevent rowid(UInt32) overflow */ \
    /** Too large part has no advantage since we cannot utilize parallelism. We set max_total_rows_to_merge as 2147483647 **/ \
    M(UInt64, max_total_rows_to_merge, 2147483647, "", 0) \
\
    M(UInt64, max_parts_to_break, 10000, "", 0)

DECLARE_SETTINGS_TRAITS(DanceMergeSelectorSettingsTraits, LIST_OF_DANCE_MERGE_SELECTOR_SETTINGS)


class DanceMergeSelectorSettings : public BaseSettings<DanceMergeSelectorSettingsTraits>
{
public:
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
};

class DanceMergeSelector : public IMergeSelector
{
public:
    using Iterator = PartsRange::const_iterator;
    using Settings = DanceMergeSelectorSettings;

    DanceMergeSelector(MergeTreeData & data_, const Settings & settings_) : data(data_), settings(settings_) {}

    PartsRange select(const PartsRanges & parts_ranges, const size_t max_total_size_to_merge) override;
    PartsRanges selectMulti(const PartsRanges & parts_ranges, const size_t max_total_size_to_merge) override;

    struct BestRangeWithScore
    {
        double min_score = std::numeric_limits<double>::max();
        Iterator best_begin;
        Iterator best_end;

        bool valid() const { return min_score != std::numeric_limits<double>::max(); }

        void update(double score, Iterator begin, Iterator end)
        {
            if (score < min_score)
            {
                min_score = score;
                best_begin = begin;
                best_end = end;
            }
        }
    };

private:
    void selectWithinPartition(const PartsRange & parts, const size_t max_total_size_to_merge);
    bool allow(double sum_size, double max_size, double min_age, double range_size);

    [[maybe_unused]] MergeTreeData & data;
    const Settings settings;

    std::unordered_map<String, size_t> num_parts_of_partitions;
    std::unordered_map<String, BestRangeWithScore> best_ranges;
};

}
