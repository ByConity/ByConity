#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS nullable_array_test;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE nullable_array_test (arr1 Nullable(Array(UInt32)), arr2 Nullable(Array(String)), arr3 Nullable(Array(Decimal(4, 2)))) ENGINE=CnchMergeTree() ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO nullable_array_test VALUES (NULL,NULL,NULL), ([],[],[])"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM nullable_array_test FORMAT Parquet" > "${CLICKHOUSE_TMP}"/nullable_array_test.parquet

${CLICKHOUSE_CLIENT} --query="SELECT * FROM nullable_array_test ORDER BY arr1"
${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE nullable_array_test"

cat "${CLICKHOUSE_TMP}"/nullable_array_test.parquet | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nullable_array_test FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM nullable_array_test ORDER BY arr1"
${CLICKHOUSE_CLIENT} --query="DROP TABLE nullable_array_test"



