#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS test;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_direct_insert;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_direct_insert(x UInt64, id UInt64) Engine=CnchMergeTree order by id;"
${CNCH_WRITE_WORKER_CLIENT} -n --query="SET prefer_cnch_catalog=1; INSERT INTO test.test_direct_insert VALUES (1, 1);"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.test_direct_insert;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_direct_insert;"

