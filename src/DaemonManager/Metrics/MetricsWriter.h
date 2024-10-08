#pragma once

#include <Core/Types.h>
#include <Server/IPrometheusMetricsWriter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>


namespace DB
{

/* Metric exporter to export metrics from DaemonManager to Prometheus.
 * Prometheus HTTP address and export interval are set in server config
 */
class DaemonManagerPrometheusMetricsWriter : public IPrometheusMetricsWriter
{
public:
    DaemonManagerPrometheusMetricsWriter() = default;
    /// Main function to export metrics
    void write(WriteBuffer & wb) override;

private:

    static inline constexpr auto DB_NAME_DELIMITER = '.'; /// database name may contain the account id, i.e. account_id.database_name

    static inline constexpr auto DAEMON_MANAGER_METRICS_PREFIX = "cnch_dm_metrics_";
};

}
