#include <Storages/MergeTree/DanceMergeSelector.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeScheduler.h>
#include <Common/Exception.h>
#include <Common/interpolate.h>

#include <cmath>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(DanceMergeSelectorSettingsTraits, LIST_OF_DANCE_MERGE_SELECTOR_SETTINGS)

void DanceMergeSelectorSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    static std::string config_elem = "dance_merge_selector";
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (auto & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in DanceMergeSelector config");
        throw;
    }
}


inline auto toPart(const void * data)
{
    return *static_cast<const std::shared_ptr<IMergeTreeDataPart> *>(data);
}

static double score(double count, double sum_size, double sum_size_fixed_cost, double count_exp)
{
    return (sum_size + sum_size_fixed_cost * count) / pow(count - 1.9, count_exp);
}

static double mapPiecewiseLinearToUnit(double value, double min, double max)
{
    return value <= min ? 0 : (value >= max ? 1 : ((value - min) / (max - min)));
}

IMergeSelector::PartsRange DanceMergeSelector::select(const PartsRanges & partitions, const size_t max_total_size_to_merge, MergeScheduler * merge_scheduler)
{
    for (auto & partition : partitions)
    {
        if (partition.size() >= 2)
            num_parts_of_partitions[toPart(partition.front().data)->info.partition_id] += partition.size();
    }

    for (auto & partition : partitions)
    {
        if (partition.size() >= 2)
            selectWithinPartition(partition, max_total_size_to_merge, merge_scheduler);
    }

    auto it = std::min_element(
        best_ranges.begin(), best_ranges.end(), [](auto & lhs, auto & rhs) { return lhs.second.min_score < rhs.second.min_score; });
    if (it != best_ranges.end() && it->second.valid())
        return PartsRange(it->second.best_begin, it->second.best_end);

    return {};
}

IMergeSelector::PartsRanges DanceMergeSelector::selectMulti(const PartsRanges & partitions, const size_t max_total_size_to_merge, MergeScheduler * merge_scheduler)
{
    for (auto & partition : partitions)
    {
        if (partition.size() >= 2)
            num_parts_of_partitions[toPart(partition.front().data)->info.partition_id] += partition.size();
    }

    for (auto & partition : partitions)
    {
        if (partition.size() >= 2)
            selectWithinPartition(partition, max_total_size_to_merge, merge_scheduler);
    }

    std::vector<BestRangeWithScore *> range_vec;
    for (auto & [_, range] : best_ranges)
    {
        if (range.valid())
            range_vec.push_back(&range);
    }
    std::sort(range_vec.begin(), range_vec.end(), [](auto * lhs, auto * rhs) { return lhs->min_score < rhs->min_score; });

    PartsRanges res;
    for (auto & range : range_vec)
        res.push_back(PartsRange(range->best_begin, range->best_end));

    return res;
}

void DanceMergeSelector::selectWithinPartition(const PartsRange & parts, const size_t max_total_size_to_merge, [[maybe_unused]] MergeScheduler * merge_scheduler)
{
    if (parts.size() <= 1)
        return;

    BestRangeWithScore current_range;

    for (size_t begin = 0; begin < parts.size(); ++begin)
    {
        /// If too many parts, select only from first, to avoid complexity.
        if (begin > settings.max_parts_to_break)
            break;

        if (!parts[begin].shall_participate_in_merges)
            continue;

        size_t sum_size = parts[begin].size;
        size_t sum_rows = parts[begin].rows;
        size_t min_age = parts[begin].age;
        size_t max_size = parts[begin].size;

        for (size_t end = begin + 2; end <= parts.size(); ++end)
        {
            if (settings.max_parts_to_merge_base && end - begin > settings.max_parts_to_merge_base)
                break;

            if (!parts[end - 1].shall_participate_in_merges)
                continue;

            size_t cur_size = parts[end - 1].size;
            size_t cur_rows = parts[end - 1].rows;
            size_t cur_age = parts[end - 1].age;

            sum_size += cur_size;
            sum_rows += cur_rows;
            min_age = std::min(min_age, cur_age);
            max_size = std::max(max_size, cur_size);

            /// LOG_TRACE(data.getLogger(), "begin " << toPart(parts[end-1].data)->name << " size " << sum_size << " rows " << sum_rows);

            if (settings.max_total_rows_to_merge && sum_rows > settings.max_total_rows_to_merge)
                break;

            if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
                break;

            if (!allow(sum_size, max_size, min_age, end - begin))
                continue;

            double current_score = score(end - begin, sum_size, settings.size_fixed_cost_to_add, settings.score_count_exp);

            size_t size_prev_at_left = begin == 0 ? 0 : parts[begin - 1].size;
            if (settings.enable_heuristic_to_align_parts
                && size_prev_at_left > sum_size * settings.heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part)
            {
                double difference = std::abs(log2(static_cast<double>(sum_size) / size_prev_at_left));
                if (difference < settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two)
                    current_score *= interpolateLinear(
                        settings.heuristic_to_align_parts_max_score_adjustment,
                        1,
                        difference / settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two);
            }

            if (end - begin >= 10
                && parts[end - 1].size < (0.001 + settings.enable_penalty_for_small_parts_at_right_ratio_base / (end - begin)))
                current_score *= settings.score_penalty_for_small_parts_at_right;

            current_range.update(current_score, parts.begin() + begin, parts.begin() + end);
        }
    }

    if (!current_range.valid())
        return;

    auto & partition_id = toPart(parts.front().data)->info.partition_id;
    auto num_parts_of_partition = num_parts_of_partitions[partition_id];
    if (num_parts_of_partition < settings.min_parts_to_enable_multi_selection)
        best_ranges["all"].update(current_range.min_score, current_range.best_begin, current_range.best_end);
    else
        best_ranges[partition_id].update(current_range.min_score, current_range.best_begin, current_range.best_end);
}

bool DanceMergeSelector::allow(double sum_size, double max_size, double min_age, double range_size)
{
    static size_t min_size_to_lower_base_log = log1p(1024 * 1024);
    static size_t max_size_to_lower_base_log = log1p(100ULL * 1024 * 1024 * 1024);

    constexpr time_t min_age_to_lower_base_at_min_size = 10;
    constexpr time_t min_age_to_lower_base_at_max_size = 10;
    constexpr time_t max_age_to_lower_base_at_min_size = 3600;
    constexpr time_t max_age_to_lower_base_at_max_size = 30 * 86400;

    /// Map size to 0..1 using logarithmic scale
    double size_normalized = mapPiecewiseLinearToUnit(log1p(sum_size), min_size_to_lower_base_log, max_size_to_lower_base_log);

    //    std::cerr << "size_normalized: " << size_normalized << "\n";

    /// Calculate boundaries for age
    double min_age_to_lower_base = interpolateLinear(min_age_to_lower_base_at_min_size, min_age_to_lower_base_at_max_size, size_normalized);
    double max_age_to_lower_base = interpolateLinear(max_age_to_lower_base_at_min_size, max_age_to_lower_base_at_max_size, size_normalized);

    //    std::cerr << "min_age_to_lower_base: " << min_age_to_lower_base << "\n";
    //    std::cerr << "max_age_to_lower_base: " << max_age_to_lower_base << "\n";

    /// Map age to 0..1
    double age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);

    //    std::cerr << "age: " << min_age << "\n";
    //    std::cerr << "age_normalized: " << age_normalized << "\n";

    double combined_ratio = std::min(1.0, age_normalized);

    //    std::cerr << "combined_ratio: " << combined_ratio << "\n";

    double lowered_base = interpolateLinear(settings.min_parts_to_merge_base, 2.0, combined_ratio);

    //    std::cerr << "------- lowered_base: " << lowered_base << "\n";

    return (sum_size + range_size * settings.size_fixed_cost_to_add) / (max_size + settings.size_fixed_cost_to_add) >= lowered_base;

}

}
