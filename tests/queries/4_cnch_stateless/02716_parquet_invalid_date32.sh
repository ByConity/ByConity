#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CURDIR/data_parquet/02716_data.parquet

${CLICKHOUSE_CLIENT} -q "drop table if exists test_out_of_range"
${CLICKHOUSE_CLIENT} -q "create table test_out_of_range (date Date32) engine = CnchMergeTree() order by tuple()"
cat $DATA_FILE | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_out_of_range format Parquet" 2>&1 | sed 's/Exception/Ex---tion/' | sed 's/host.*:\ Input/host = 127.0.0.1:\ Input/'
${CLICKHOUSE_CLIENT} -q "drop table test_out_of_range"

${CLICKHOUSE_CLIENT} -q "drop table if exists test_out_of_range_use_int"
${CLICKHOUSE_CLIENT} -q "create table test_out_of_range_use_int (date Int32) engine = CnchMergeTree() order by tuple()"
cat $DATA_FILE | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test_out_of_range_use_int format Parquet" 
${CLICKHOUSE_CLIENT} -q "select * from test_out_of_range_use_int"
${CLICKHOUSE_CLIENT} -q "drop table test_out_of_range_use_int"
