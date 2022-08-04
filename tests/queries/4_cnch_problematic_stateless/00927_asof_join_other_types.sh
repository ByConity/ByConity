#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for typename in "UInt32" "UInt64" "Float64" "Float32"
do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.A;"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test.B;"

    $CLICKHOUSE_CLIENT -q "CREATE TABLE test.A(k UInt32, t ${typename}, a Float64) ENGINE = CnchMergeTree() ORDER BY (k, t);"
    $CLICKHOUSE_CLIENT -q "INSERT INTO test.A(k,t,a) VALUES (2,1,1),(2,3,3),(2,5,5);"

    $CLICKHOUSE_CLIENT -q "CREATE TABLE test.B(k UInt32, t ${typename}, b Float64) ENGINE = CnchMergeTree() ORDER BY (k, t);"
    $CLICKHOUSE_CLIENT -q "INSERT INTO test.B(k,t,b) VALUES (2,3,3);"

    $CLICKHOUSE_CLIENT -q "SELECT k, t, a, b FROM test.A ASOF LEFT JOIN test.B USING(k,t) ORDER BY (k,t);"
done