#pragma once

#include <Common/Logger.h>
#include <stddef.h>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <Core/Types.h>
#include <Poco/Logger.h>
#include <Common/LabelledMetrics.h>
#include <metric_helper.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

/** Implements histogram metrics, which can be helpful for latency etc.
  * Metric name, documentation and histogram buckets are to be defined in HistogramMetrics.cpp
  * Use the increment(HistogramMetric, value) function to record metrics
  * Metrics are stored in HistogramMetrics::sums and HistogramMetrics::values
  *
  */
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace HistogramMetrics
{
    using Metric = size_t;
    using Count = size_t;
    using Value = DB::Int64;
    using Histogram = std::vector<Count>;
    using MetricLabels = LabelledMetrics::MetricLabels;

    struct HistogramMetric
    {
        Histogram values;
        Value sum;
    };

    using LabelledHistogram = std::unordered_map<MetricLabels, HistogramMetric, LabelledMetrics::MetricLabelHasher>;

    extern LabelledHistogram histogram_metrics[];
    extern std::mutex metric_mutexes[];

    /// Get name of metric by identifier. Returns statically allocated string.
    const char * getName(Metric event);
    /// Get name of metric by identifier in snake_case. Returns statically allocated string.
    const DB::String getSnakeName(Metric event);
    /// Get text description of metric by identifier. Returns statically allocated string.
    const char * getDocumentation(Metric event);
    /// Get histogram bucket limits of metric
    const std::vector<Value> getHistogramBucketLimits(Metric metric);

    const std::unique_lock<std::mutex> getLock(Metric metric);

    inline void increment(Metric metric, Value value, Metrics::MetricType type = Metrics::MetricType::None, const LabelledMetrics::MetricLabels & labels = {})
    {
        auto histogram = getHistogramBucketLimits(metric);
        size_t i = 0;
        while (i < histogram.size())
        {
            if (value <= histogram[i])
                break;
            ++i;
        }

        auto & hist_metric = histogram_metrics[metric][labels];

        {
            auto lock = getLock(metric);
            if (hist_metric.values.empty())
            {
                hist_metric.values = Histogram(histogram.size());
                hist_metric.sum = 0;
            }

            hist_metric.values[i] += 1;
            hist_metric.sum += value;
        }

        if (type != Metrics::MetricType::None)
        {
            if (type != Metrics::MetricType::Timer)
            {
                LOG_ERROR(getLogger("HistogramMetrics"), "Only support Metrics::MetricType::Timer type when report histogram metrics");
                return;
            }

            try
            {
                Metrics::EmitTimer(getSnakeName(metric), value, LabelledMetrics::toString(labels));
            }
            catch (DB::Exception & e)
            {
                LOG_ERROR(getLogger("HistogramMetrics"), "Metrics emit metric failed: {}", e.message());
            }
        }
    }
}
