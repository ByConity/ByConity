#include <Server/IPrometheusMetricsWriter.h>
#include <Common/config_version.h>
#include <common/logger_useful.h>

namespace DB
{
void IPrometheusMetricsWriter::writeConfigMetrics(WriteBuffer & wb)
{
    for (const auto & [metric_name, metric_doc] : config_namedoc_map)
    {
        std::string key{CONFIG_METRIC_PREFIX + metric_name};
        std::string key_label(key);
        writeOutLine(wb, "# HELP", key, metric_doc);
        writeOutLine(wb, "# TYPE", key, COUNTER_TYPE);

        if (metric_name == BUILD_INFO_KEY)
        {
            MetricLabels labels {
                {"revision", std::to_string(revision)},
                {"version_scm", VERSION_SCM},
                {"version_githash", VERSION_GITHASH}};

            key_label += getLabel(labels);
            writeOutLine(wb, key_label, 1); //Output arbitary value as only the labels are useful
        }
        else
        {
            LOG_WARNING(getLogger("IPrometheusMetricsWriter"), "Unknown config metric found, this should never happen");
            writeOutLine(wb, key_label, 0);
        }
    }
}

}
