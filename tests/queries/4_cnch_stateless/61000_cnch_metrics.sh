#!/usr/bin/env bash

#--------------------------------------------
# this case is for guranteeing the byteyard dependent metrics
#--------------------------------------------

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

####
echo cnch_profile_events_labelled_query_total
$CLICKHOUSE_CLIENT --query="CREATE DATABASE IF NOT EXISTS test_metrics";
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_metrics.t";
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_metrics.t (id INT) ENGINE = CnchMergeTree() ORDER BY id";

for i in `seq 1 5`; do $CLICKHOUSE_CLIENT --query="INSERT INTO test_metrics.t SELECT * FROM numbers(100) SETTINGS virtual_warehouse = 'vw_default'"; done
for i in `seq 1 5`; do $CLICKHOUSE_CLIENT --query="SELECT * FROM test_metrics.t FORMAT Null SETTINGS virtual_warehouse = 'vw_default'"; done
for i in `seq 1 5`; do $CLICKHOUSE_CLIENT --query="SELECT * FROM test_metrics.t FORMAT Null SETTINGS virtual_warehouse = 'vw_default', enable_optimizer=0"; done

${CLICKHOUSE_CURL} -s ${CLICKHOUSE_PURE_URL}metrics \
| grep ^cnch_profile_events_labelled_query_total \
| grep vw_default \
| sort \
| awk -F'[{}]' '{split($2, kv, ","); for (i in kv) {split(kv[i], pair, "="); printf("%s: %s\n", pair[1], pair[2]);} \
                value=$NF; if (value > 0) {print "Value is greater than 0"} else {print "Value is not greater than 0"}  }'

####
echo cnch_current_metrics_query
for i in `seq 1 5`; do $CLICKHOUSE_CLIENT --query="SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null" & done
sleep 10

${CLICKHOUSE_CURL} -s ${CLICKHOUSE_PURE_URL}metrics \
| grep ^cnch_current_metrics_query \
| grep default \
| sort \
| awk -F'[{}]' '{split($2, kv, ","); for (i in kv) {split(kv[i], pair, "="); printf("%s: %s\n", pair[1], pair[2]);} \
                value=$NF; if (value > 0) {print "Value is greater than 0"} else {print "Value is not greater than 0"}  }'

####
echo cnch_profile_events_queries_failed_total
$CLICKHOUSE_CLIENT --query="SELECT throwIf(id) FROM test_metrics.t" 2>/dev/null;
 
n=`${CLICKHOUSE_CURL} -s ${CLICKHOUSE_PURE_URL}metrics \
| grep ^cnch_profile_events_queries_failed_total \
| grep vw_default \
| sort \
| wc -l`
echo "cnch_profile_events_queries_failed_total check $((n>0?1:0))"

####
echo cnch_histogram_metrics_query_latency_count
${CLICKHOUSE_CURL} -s ${CLICKHOUSE_PURE_URL}metrics \
| grep ^cnch_histogram_metrics_query_latency_count \
| grep vw_default \
| sort \
| awk -F'[{}]' '{value=$NF; if (value > 0) {print "Value is greater than 0"} else {print "Value is not greater than 0"} }'


####
echo cnch_histogram_metrics_query_latency_bucket
${CLICKHOUSE_CURL} -s ${CLICKHOUSE_PURE_URL}metrics \
| grep ^cnch_histogram_metrics_query_latency_bucket \
| grep vw_default \
| grep Inf \
| sort \
| awk -F'[{}]' '{value=$NF; if (value > 0) {print "Value is greater than 0"} else {print "Value is not greater than 0"} }'

