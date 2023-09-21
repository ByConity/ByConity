#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh
$CLICKHOUSE_CLIENT -m -n --query="
DROP TABLE IF EXISTS t1_48021;
DROP TABLE IF EXISTS t2_48021;
DROP TABLE IF EXISTS t3_48021;
DROP TABLE IF EXISTS t4_48021;
CREATE TABLE t1_48021 (a UInt64, b UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE t2_48021 (a UInt64, c UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE t3_48021 (a UInt64, d UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
CREATE TABLE t4_48021 (a UInt64, e UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY a;
INSERT INTO t1_48021 (a, b) VALUES (1, 1);
INSERT INTO t2_48021 (a, c) VALUES (1, 1);
INSERT INTO t3_48021 (a, d) VALUES (1, 1);
INSERT INTO t4_48021 (a, e) VALUES (1, 1);
SELECT * FROM t1_48021 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1 FORMAT JSON;
SELECT sum(b) FROM t1_48021 GROUP BY a SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1 FORMAT JSON;
SELECT * FROM t1_48021,t2_48021,t3_48021,t4_48021 WHERE t1_48021.a=t2_48021.a and t2_48021.a=t3_48021.a and t3_48021.a=t4_48021.a SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1 FORMAT JSON;
" | grep -E "rows_read|bytes_read";

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=100000" -d 'SELECT * FROM t1_48021 SETTINGS enable_optimizer=1,enable_wait_for_post_processing=1 FORMAT JSON' 2>&1 | grep 'X-ClickHouse-Progress'

$CLICKHOUSE_CLIENT -m -n --query="
DROP TABLE t1_48021;
DROP TABLE t2_48021;
DROP TABLE t3_48021;
DROP TABLE t4_48021;
";