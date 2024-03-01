#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS  nested_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS  nested_struct_test"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE nested_table (id Bigint, address Tuple(x float, y float)) engine=CnchMergeTree() order by tuple()"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE nested_struct_test (id Bigint, address Array(Tuple(x float, y float))) engine=CnchMergeTree() order by tuple()"


cat $CUR_DIR/data_orc_arrow_parquet_nested/part-00000-02f9fe93-a335-40ca-966d-be98d2856e85-c000.gz.parquet | ${CLICKHOUSE_CLIENT} -q "INSERT INTO  nested_table FORMAT Parquet SETTINGS input_format_parquet_import_nested = 1"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_table"


cat $CUR_DIR/data_orc_arrow_parquet_nested/part-00000-4c28b61e-c361-4923-b541-e2bf00e04fd8-c000.gz.parquet | ${CLICKHOUSE_CLIENT} -q "INSERT INTO  nested_struct_test FORMAT Parquet SETTINGS input_format_parquet_import_nested = 1"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_struct_test"


${CLICKHOUSE_CLIENT} --query="DROP TABLE nested_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE nested_struct_test"
