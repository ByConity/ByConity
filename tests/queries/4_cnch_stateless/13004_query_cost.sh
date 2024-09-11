#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -m -n --query="
CREATE DATABASE IF NOT EXISTS test_query_cost_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t1_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t2_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t3_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t4_48021;
CREATE TABLE test_query_cost_48021.t1_48021 (a UInt64, b UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE test_query_cost_48021.t2_48021 (a UInt64, c UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE test_query_cost_48021.t3_48021 (a UInt64, d UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE test_query_cost_48021.t4_48021 (a UInt64, e UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
INSERT INTO test_query_cost_48021.t1_48021 (a, b) VALUES (1, 1);
INSERT INTO test_query_cost_48021.t2_48021 (a, c) VALUES (1, 1);
INSERT INTO test_query_cost_48021.t3_48021 (a, d) VALUES (1, 1);
INSERT INTO test_query_cost_48021.t4_48021 (a, e) VALUES (1, 1);
SELECT * FROM test_query_cost_48021.t1_48021 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000,interactive_delay=10000000 FORMAT JSON;
SELECT sum(b) FROM test_query_cost_48021.t1_48021 GROUP BY a SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000,interactive_delay=10000000 FORMAT JSON;
SELECT * FROM test_query_cost_48021.t1_48021,test_query_cost_48021.t2_48021,test_query_cost_48021.t3_48021,test_query_cost_48021.t4_48021 WHERE test_query_cost_48021.t1_48021.a=test_query_cost_48021.t2_48021.a and test_query_cost_48021.t2_48021.a=test_query_cost_48021.t3_48021.a and test_query_cost_48021.t3_48021.a=test_query_cost_48021.t4_48021.a SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000,interactive_delay=10000000 FORMAT JSON;
SELECT a FROM test_query_cost_48021.t1_48021 UNION ALL SELECT 1 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000,interactive_delay=10000000 FORMAT JSON;
" | grep -E "rows_read|bytes_read";

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=100000" -d 'SELECT * FROM test_query_cost_48021.t1_48021 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000 FORMAT JSON' 2>&1 | grep 'X-ClickHouse-Summary'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=100000" -d 'SELECT * FROM test_query_cost_48021.t1_48021 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1,wait_for_post_processing_timeout_ms=10000 FORMAT JSON' 2>&1 | grep -E "rows_read|bytes_read"

$CLICKHOUSE_CLIENT -m -n --query="
DROP TABLE IF EXISTS test_query_cost_48021.t1_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t2_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t3_48021;
DROP TABLE IF EXISTS test_query_cost_48021.t4_48021;
DROP DATABASE IF EXISTS test_query_cost_48021;
";