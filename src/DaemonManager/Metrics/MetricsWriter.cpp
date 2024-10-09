#include "MetricsWriter.h"
#include <DaemonManager/Metrics/MetricsData.h>

namespace DB {
void DaemonManagerPrometheusMetricsWriter::write(WriteBuffer & wb)
{
    writeConfigMetrics(wb);

    String oldest_transaction_ts{DAEMON_MANAGER_METRICS_PREFIX};
    oldest_transaction_ts.append("oldest_transaction"); // create key with prefix
    int64_t oldest_transaction_timestamp = oldest_transaction; // get value for gauge
    String oldest_transaction_ts_label{oldest_transaction_ts};
    String oldest_transaction_ts_doc = "Oldest Transaction Timestamp, useful to track the lag of TR/UB cleaning."; // create doc for #help


    // Write out metrics to prometheus
    writeOutLine(wb, "# HELP", oldest_transaction_ts, oldest_transaction_ts_doc);
    writeOutLine(wb, "# TYPE", oldest_transaction_ts, "gauge");
    writeOutLine(wb, oldest_transaction_ts_label, oldest_transaction_timestamp);
}
}
