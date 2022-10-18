#pragma once
#include <Core/Settings.h>
namespace DB::Statistics
{
struct CollectorSettings
{
    bool collect_debug_level;
    bool collect_histogram;
    bool collect_floating_histogram;
    bool collect_floating_histogram_ndv;
    bool enable_sample;
    UInt64 sample_row_count;
    double sample_ratio;

    explicit CollectorSettings(const Settings & settings)
    {
        collect_debug_level = settings.statistics_collect_debug_level;
        collect_histogram = settings.statistics_collect_histogram;
        collect_floating_histogram = settings.statistics_collect_floating_histogram;
        collect_floating_histogram_ndv = settings.statistics_collect_floating_histogram_ndv;
        enable_sample = settings.statistics_enable_sample;
        sample_row_count = settings.statistics_sample_row_count;
        sample_ratio = settings.statistics_sample_ratio;
    }
};

}
