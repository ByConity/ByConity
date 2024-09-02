#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ingest_cc"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ingest_cc (p Date, k Int64, v1 Int64, v2 Int64, v3 Int64) engine=CnchMergeTree partition by p order by k primary key k cluster by k into 4 buckets"
# make sure merge thread is running, which is required by ingest partition
${CLICKHOUSE_CLIENT} --query="SYSTEM START MERGES ingest_cc"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ingest_cc (p, k) SELECT '2024-01-01', number FROM numbers(20)"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ingest_cc_src AS ingest_cc"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ingest_cc_src (p, k, v1, v2, v3) SELECT '2024-01-01', number + 15, 1, 2, 3 FROM numbers(10)"

# run 3 ingests in parallel
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ingest_cc ingest partition id '20240101' COLUMNS v1 FROM ingest_cc_src SETTINGS enable_memory_efficient_ingest_partition=1, ingest_column_memory_lock_timeout=60, sleep_in_send_ingest_to_worker_ms=2000" &
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ingest_cc ingest partition id '20240101' COLUMNS v2 FROM ingest_cc_src SETTINGS enable_memory_efficient_ingest_partition=1, ingest_column_memory_lock_timeout=60, sleep_in_send_ingest_to_worker_ms=2000" &
${CLICKHOUSE_CLIENT} --query="ALTER TABLE ingest_cc ingest partition id '20240101' COLUMNS v3 FROM ingest_cc_src SETTINGS enable_memory_efficient_ingest_partition=1, ingest_column_memory_lock_timeout=60, sleep_in_send_ingest_to_worker_ms=2000" &
wait

${CLICKHOUSE_CLIENT} --query="SELECT * FROM ingest_cc ORDER BY k"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ingest_cc"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ingest_cc_src"
