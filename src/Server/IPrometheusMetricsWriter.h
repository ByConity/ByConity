#pragma once

#include <Common/LabelledMetrics.h>
#include <Common/ClickHouseRevision.h>
#include <IO/WriteHelpers.h>

using MetricLabels = LabelledMetrics::MetricLabels;

namespace
{

template <typename T>
void writeOutLine(DB::WriteBuffer & wb, T && val)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar('\n', wb);
}

template <typename T, typename... TArgs>
void writeOutLine(DB::WriteBuffer & wb, T && val, TArgs &&... args)
{
    DB::writeText(std::forward<T>(val), wb);
    DB::writeChar(' ', wb);
    writeOutLine(wb, std::forward<TArgs>(args)...);
}

inline std::string getLabel(const MetricLabels & labels)
{
    if (labels.empty())
        return std::string{};
    else if (labels.size() == 1)
    {
        auto label = labels.begin();
        return "{" + label->first + "=\"" + label->second + "\"}";
    }
    else
    {
        std::string res{"{"};
        auto it = labels.begin();
        while (it != labels.end())
        {
            const auto & label = *it;
            res += label.first + "=\"" + label.second + "\"";
            ++it;
            if (it != labels.end())
                res += ",";
        }
        res += "}";
        return res;
    }
}

inline void replaceInvalidChars(std::string & metric_name)
{
    std::replace(metric_name.begin(), metric_name.end(), '.', '_');
}

}

namespace DB
{

class WriteBuffer;


/** Parent class for Metric exporter to export metrics to Prometheus.
 */
class IPrometheusMetricsWriter
{
public:

    /// Main function to export metrics
    virtual void write(WriteBuffer & wb) = 0;
    virtual ~IPrometheusMetricsWriter() = default;

protected:
    virtual void writeConfigMetrics(WriteBuffer & wb);

    // Metric types
    static constexpr auto COUNTER_TYPE = "counter";
    static constexpr auto HISTOGRAM_TYPE = "histogram";
    static constexpr auto GAUGE_TYPE = "gauge";

    static constexpr auto CONFIG_METRIC_PREFIX = "cnch_config_";

    const UInt32 revision = ClickHouseRevision::getVersionRevision();
    static constexpr auto BUILD_INFO_KEY = "build_info";

private:
    const std::unordered_map<String, String> config_namedoc_map =
    {
        {BUILD_INFO_KEY, "Build info"},
    };

};

}
