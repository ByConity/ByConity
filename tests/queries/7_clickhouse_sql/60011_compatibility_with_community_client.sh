#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$COMMUNITY_CLICKHOUSE_CLIENT --query="CREATE DATABASE IF NOT EXISTS compatibility_test;"
$COMMUNITY_CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS compatibility_test.table;"
$COMMUNITY_CLICKHOUSE_CLIENT --query="CREATE TABLE compatibility_test.table(id UInt64, a UInt64, b Int32, c String) ENGINE = CnchMergeTree() ORDER BY id;"
$COMMUNITY_CLICKHOUSE_CLIENT --query="INSERT INTO compatibility_test.table VALUES (1, 100, -100, 'clickhouse'), (2, 3, 4, 'database'), (5, 6, 7, 'columns'), (10, 9, 8, '');"
$COMMUNITY_CLICKHOUSE_CLIENT --query="SELECT * FROM compatibility_test.table ORDER BY id;"

$COMMUNITY_CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS compatibility_test.table;"
$COMMUNITY_CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS compatibility_test;"
