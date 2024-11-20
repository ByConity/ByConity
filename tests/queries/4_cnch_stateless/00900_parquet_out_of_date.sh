#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_parquet/test_parquet_date_out_of_range/000000_0_copy_2

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_parquet_date_out_of_range32;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_parquet_date_out_of_range32(user_id Int64, last_login_time Date32) ENGINE = CnchMergeTree ORDER BY tuple()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_parquet_date_out_of_range32 format Parquet"  2>&1 | sed 's/Exception/Ex---tion/' | sed 's/host.*:\ Input/host = 127.0.0.1:\ Input/'

cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_parquet_date_out_of_range32 format Parquet SETTINGS date_time_overflow_behavior = 'saturate' " 

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.test_parquet_date_out_of_range32;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_parquet_date_out_of_range32;"



${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_parquet_date_out_of_range;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.test_parquet_date_out_of_range(user_id Int64, last_login_time Date) ENGINE = CnchMergeTree ORDER BY tuple()"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "INSERT INTO test.test_parquet_date_out_of_range format Parquet"  2>&1 | sed 's/Exception/Ex---tion/' | sed 's/host.*:\ Input/host = 127.0.0.1:\ Input/'

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.test_parquet_date_out_of_range;"