#pragma once

#include <string>
#include <Core/Types.h>
#include <Server/IPrometheusMetricsWriter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <TSO/TSOServer.h>
#include <IO/WriteBuffer.h>
#include <unordered_map>
#include <Interpreters/Context.h>

#define DB_NAME_DELIMITER '.'  /// database name may contain the account id, i.e. account_id.database_name

namespace DB
{

/* Metric exporter to export metrics from TSO to Prometheus.
 * Prometheus HTTP address and export interval are set in server config
 */
class TSOPrometheusMetricsWriter : public IPrometheusMetricsWriter
{
public:
    TSOPrometheusMetricsWriter(
        const Poco::Util::AbstractConfiguration & config,
        const TSO::TSOServer & tso_server_,
        const ContextPtr context_,
        const std::string & config_name);

    /// Main function to export metrics
    void write(WriteBuffer & wb) override;

private:

    const TSO::TSOServer & tso_server;
    const ContextPtr context;

    static inline constexpr auto CONFIG_METRIC_PREFIX = "cnch_config_";

    static inline constexpr auto YIELD_LEADERSHIP_KEY = "num_yielded_leadership";
    static inline constexpr auto IS_LEADER_KEY = "is_leader";
    static inline constexpr auto UPDATE_TS_STOPPED_KEY = "num_tso_update_ts_stopped";

    const std::unordered_map<String, String> metrics_namedoc_map =
    {
        {YIELD_LEADERSHIP_KEY, "Number of times leadership was yielded by this TSO node."},
        {IS_LEADER_KEY, "Denotes if this TSO node is a leader."},
        {UPDATE_TS_STOPPED_KEY, "Number of times TSO update timestamp stopped functioning."},
    };

    const bool send_metrics;

    static inline constexpr auto TSO_METRICS_PREFIX = "cnch_tso_metrics_";
};

}
