#pragma once
#include <unordered_map>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <boost/lexical_cast.hpp>

namespace DB::Statistics
{
// clang-format off

// type, name, default_value, description, is_internal
// settings used for creating stats
#define CREATE_STATS_SETTINGS(M) \
    M(Bool, collect_histogram, true, "Enable histogram collection", 0) \
    M(Bool, collect_floating_histogram, true, "Collect histogram for float/double/Decimal columns", 0) \
    M(Bool, collect_floating_histogram_ndv, true, "Collect histogram ndv for float/double/Decimal columns", 0) \
    M(UInt64, collect_string_size_limit_for_histogram, 64, "Collect string histogram only for avg_size <= string_size_limit, since it's unnecessary to collect stats for text", 0) \
    M(UInt64, histogram_bucket_size, 250, "Default bucket size of histogram", 0) \
    M(UInt64, kll_sketch_log_k, DEFAULT_KLL_SKETCH_LOG_K, "Default logK parameter of kll_sketch in statistics", 0) \
    M(Bool, enable_sample, true, "Use sampling for statistics", 0) \
    M(UInt64, sample_row_count, 40'000'000, "Minimal row count for sampling", 0) \
    M(Float, sample_ratio, 0.001, "Ratio for sampling", 0) \
    M(StatisticsAccurateSampleNdvMode, accurate_sample_ndv, StatisticsAccurateSampleNdvMode::AUTO, "Mode of accurate sample ndv to estimate full ndv", 0) \
    M(UInt64, accurate_sample_ndv_row_limit, 40'000'000, "Limit of accurate sample ndv sample row count, to limit create stats cost. 0 for unlimited", 0) \
    M(UInt64, batch_max_columns, 30, "Max column size in a batch when collecting stats", 0) \
    M(String, exclude_tables_regex, "", "Regex to exclude tables for statistics operations", 0) \
    M(Bool, if_not_exists, false, "Do collection if not exists, otherwise always do collections", 0) \
    M(StatisticsCachePolicy, cache_policy, StatisticsCachePolicy::Default, "Cache policy for stats command and SQLs: (default|cache|catalog)", 0) \
    M(Bool, collect_in_partitions, false, "collect partitioned stats", 0) \
    M(UInt64, max_partitions_in_a_batch, 1000, "Max parallel size of union all when collect partitioned stats", 0) \
    M(Int64, ignore_modified_timestamp_older_than, 0, "Ignore partitions whose modified_time older than this Unix timestamp. 0 for unlimited, negative value for now() - abs(value)", 0) \
    M(UInt64, max_partitions, 0, "Max partitions in total to collect partitioned stats, 0 for unlimited", 0) \

// settings only useful for show stats and query sql
#define USE_STATS_SETTINGS(M) \
    M(Bool, simplify_histogram, false, "Reduce buckets of histogram with simplifying", 1) \
    M(Float, simplify_histogram_ndv_density_threshold, 0.2, "Histogram simplifying threshold for ndv", 1) \
    M(Float, simplify_histogram_range_density_threshold, 0.2, "Histogram simplifying threshold for range", 1) \
    M(StatisticsCachePolicy, cache_policy, StatisticsCachePolicy::Default, "Cache policy for stats command and SQLs: (default|cache|catalog)", 1) \
    M(Bool, return_row_count_if_empty, true, "Return row count using count(*) if stats is empty", 1) \

// type, name, default_value, description, is_internal
// settings to control behaviour of auto stats manager
#define AUTO_STATS_MANAGER_SETTINGS(M) \
    M(String, collect_window, "00:00-23:59", "Time window to do auto stats collect, using timezone on server", 0) \
    M(Bool, collect_empty_stats_immediately, false, "If statistics is empty, ignore collect_window and start statistics collection immediately", 0) \
    M(UInt64, auto_stats_max_retry_times, 3, "Max retry times for failed auto stats task", 0) \
    M(UInt64, schedule_period_seconds, 60, "Schedule internal for auto statistics manager", 0) \
    M(UInt64, task_expire_days, 7, "Too old task log is considered expired after these days", 0) \
    M(Bool, enable_auto_stats, false, "Enable auto statistics", 0) \
    M(Bool, enable_async_tasks, true, "Enable async tasks", 0) \
    M(UInt64, update_row_count_threshold, 1'000'000, "Threshold of ratio of row count changes to trigger auto stats", 0) \
    M(Float, update_ratio_threshold, 0.2, "Threshold of minimal row count to trigger auto stats", 0) \
    M(UInt64, update_interval_seconds, 1200, "The minimal interval between two auto stats collection task on the same table", 0) \
    M(Bool, enable_scan_all_tables, true, "All tables will be scanned for auto-collection when stats is unhealthy comparing to real row count", 0) \
    M(UInt64, scan_all_tables_interval_seconds, 3600, "The minimal interval between two tasks of scanning all tables", 0) \


// NOLINTNEXTLINE
// clang-format on 
class SettingsMap : public std::unordered_map<String, String>
{
public:
    explicit SettingsMap() = default;
    explicit SettingsMap(std::unordered_map<String, String> tmp) : unordered_map(std::move(tmp))
    {
    }
    String toJsonStr() const;
    void fromJsonStr(const String & json_str);
};

#define GET_SET(TYPE, NAME, DEFAULT_VALUE, EXPLAIN, INTERNAL) \
    SettingField##TYPE::Type NAME() const \
    { \
        SettingField##TYPE result{DEFAULT_VALUE}; \
        if (auto iter = this->find(#NAME); iter != this->end()) \
        { \
            result.parseFromString(iter->second); \
        } \
        return result; \
    } \
    void set_##NAME(SettingField##TYPE::Type value) \
    { \
        SettingField##TYPE tmp(value); \
        auto str = tmp.toString(); \
        (*this)[#NAME] = std::move(str); \
    }

class CreateStatsSettings : public SettingsMap
{
public:
    CreateStatsSettings() = default;
    void fromContextSettings(const Settings & settings);
    void normalize();

    CREATE_STATS_SETTINGS(GET_SET)
};

class AutoStatsManagerSettings : public SettingsMap
{
public:
    AutoStatsManagerSettings() = default;
    using SettingsMap::SettingsMap;
    void applySettingsChanges(const SettingsChanges & changes, bool throw_if_override = false);

    AUTO_STATS_MANAGER_SETTINGS(GET_SET)
};
#undef GET_SET

struct StatisticsScope
{
    std::optional<String> database; // nullopt for all
    std::optional<String> table; // nullopt for all
};

// only for statistics
void applyStatisticsSettingsChanges(Settings & settings, SettingsChanges settings_changes);

}
