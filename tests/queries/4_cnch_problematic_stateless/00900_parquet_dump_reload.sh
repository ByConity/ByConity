#!/usr/bin/env bash

# Test for nested data type like array and map. Dump selected data as parquet format and reload.


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.dump_parquet"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.reload_parquet"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.dump_parquet(id UInt32, arr_data Array(Int32), map_data Map(String, String)) ENGINE=CnchMergeTree PARTITION BY id ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.reload_parquet AS test.dump_parquet"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.dump_parquet VALUES (1001, [1,2,3,4], {'a1':'va1', 'a2':'va2'}), (1002, [5,6], {'b1':'vb1','b2':'vb2','b3':'vb3'})"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.dump_parquet FORMAT Parquet" > ${CLICKHOUSE_TMP}/parquet_dump_reload.parquet
cat ${CLICKHOUSE_TMP}/parquet_dump_reload.parquet | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.reload_parquet FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.reload_parquet ORDER BY id"

rm -f ${CLICKHOUSE_TMP}/parquet_dump_reload.parquet
