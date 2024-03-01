#!/usr/bin/env bash
#Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists  test_parquet_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "create table  test_parquet_allow_miss_col (x UInt64, y String default 'Hello') engine = CnchMergeTree() order by tuple()"
${CLICKHOUSE_CLIENT} -q "select number as x from numbers(3) format Parquet" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO  test_parquet_allow_miss_col format Parquet SETTINGS input_format_parquet_allow_missing_columns = 1" 

${CLICKHOUSE_CLIENT} -q "select y from  test_parquet_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "select count(*), count(y) from  test_parquet_allow_miss_col"

${CLICKHOUSE_CLIENT} -q "drop table  test_parquet_allow_miss_col"

${CLICKHOUSE_CLIENT} -q "drop table if exists  test_native_orc_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "create table  test_native_orc_allow_miss_col (x UInt64, y String default 'Hello') engine = CnchMergeTree() order by tuple()"
${CLICKHOUSE_CLIENT} -q "select number as x from numbers(3) format ORC" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO  test_native_orc_allow_miss_col format ORC SETTINGS input_format_orc_allow_missing_columns = 1" 
${CLICKHOUSE_CLIENT} -q "select y from  test_native_orc_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "select count(*), count(y) from  test_native_orc_allow_miss_col"

${CLICKHOUSE_CLIENT} -q "drop table  test_native_orc_allow_miss_col"

${CLICKHOUSE_CLIENT} -q "drop table if exists  test_orc_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "create table  test_orc_allow_miss_col (x UInt64, y String default 'Hello') engine = CnchMergeTree() order by tuple()"
${CLICKHOUSE_CLIENT} -q "select number as x from numbers(3) format ORC" | ${CLICKHOUSE_CLIENT} -q "INSERT INTO  test_orc_allow_miss_col format ORC SETTINGS input_format_orc_allow_missing_columns = 1" 
${CLICKHOUSE_CLIENT} -q "select y from  test_orc_allow_miss_col"
${CLICKHOUSE_CLIENT} -q "select count(*), count(y) from  test_orc_allow_miss_col"

${CLICKHOUSE_CLIENT} -q "drop table  test_orc_allow_miss_col"

# TODO: fix FILE ENGINE
# $CLICKHOUSE_LOCAL -q "select y from file(02511_data1.parquet, auto, 'x UInt64, y String default \'Hello\'') settings input_format_parquet_allow_missing_columns=1"
# $CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format Parquet" > 02511_data2.parquet
# $CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('02511_data*.parquet', auto, 'x UInt64, y String') settings input_format_parquet_allow_missing_columns=1"

# $CLICKHOUSE_LOCAL -q "select number as x from numbers(3) format ORC" > 02511_data1.orc
# $CLICKHOUSE_LOCAL -q "select y from file(02511_data1.orc, auto, 'x UInt64, y String default \'Hello\'') settings input_format_orc_allow_missing_columns=1"
# $CLICKHOUSE_LOCAL -q "select number as x, 'Hello' as y from numbers(3) format ORC" > 02511_data2.orc
# $CLICKHOUSE_LOCAL -q "select count(*), count(y) from file('02511_data*.orc', auto, 'x UInt64, y String') settings input_format_orc_allow_missing_columns=1"



# rm 02511_data*

