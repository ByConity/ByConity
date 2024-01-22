#include <Server/TSOPrometheusMetricsWriter.h>

#include <algorithm>

namespace DB
{

TSOPrometheusMetricsWriter::TSOPrometheusMetricsWriter(
    const Poco::Util::AbstractConfiguration & config,
    const TSO::TSOServer & tso_server_,
    const ContextPtr context_,
    const std::string & config_name)
    : tso_server(tso_server_)
    , context(context_)
    , send_metrics(config.getBool(config_name + ".metrics", true))
{}

void TSOPrometheusMetricsWriter::write(WriteBuffer & wb)
{
    writeConfigMetrics(wb);
    String yielded_leadership_key{TSO_METRICS_PREFIX};
    yielded_leadership_key.append(YIELD_LEADERSHIP_KEY); // create key with prefix
    int yielded_leadership_count = tso_server.getNumYieldedLeadership(); // get value for gauge
    String yielded_leadership_key_label{yielded_leadership_key};
    yielded_leadership_key_label += getLabel({{"tso_endpoint", tso_server.getHostPort()}}); // create label for label-value pair
    auto yielded_leadership_metric_doc = metrics_namedoc_map.at(YIELD_LEADERSHIP_KEY); // create doc for #help

    // TODO: implement get TSO leader host port using zookeeper
    String is_leader_key{TSO_METRICS_PREFIX};
    is_leader_key.append(IS_LEADER_KEY); // create key with prefix
    String is_leader_key_label{is_leader_key};
    is_leader_key_label += getLabel({{"tso_leader_endpoint", tso_server.tryGetTSOLeaderHostPort()}}); // create label for label-value pair
    auto is_leader_metric_doc = metrics_namedoc_map.at(IS_LEADER_KEY); // create doc for #help

    String update_ts_stopped_key{TSO_METRICS_PREFIX};
    update_ts_stopped_key.append(UPDATE_TS_STOPPED_KEY); // create key with prefix
    int update_ts_stopped_count = tso_server.getNumStopUpdateTsFromTSOService(); // get value for gauge
    String update_ts_stopped_key_label{update_ts_stopped_key};
    update_ts_stopped_key_label += getLabel({{"tso_endpoint", tso_server.getHostPort()}}); // create label for label-value pair
    auto update_ts_stopped_key_metric_doc = metrics_namedoc_map.at(UPDATE_TS_STOPPED_KEY); // create doc for #help
    

    // write out metrics to prometheus
    writeOutLine(wb, "# HELP", yielded_leadership_key, yielded_leadership_metric_doc);
    writeOutLine(wb, "# TYPE", yielded_leadership_key, "gauge");
    writeOutLine(wb, yielded_leadership_key_label, yielded_leadership_count);

    writeOutLine(wb, "# HELP", is_leader_key, is_leader_metric_doc);
    writeOutLine(wb, "# TYPE", is_leader_key, "gauge");
    writeOutLine(wb, is_leader_key_label, tso_server.isLeader());

    writeOutLine(wb, "# HELP", update_ts_stopped_key, update_ts_stopped_key_metric_doc);
    writeOutLine(wb, "# TYPE", update_ts_stopped_key, "gauge");
    writeOutLine(wb, update_ts_stopped_key_label, update_ts_stopped_count);

}

}
