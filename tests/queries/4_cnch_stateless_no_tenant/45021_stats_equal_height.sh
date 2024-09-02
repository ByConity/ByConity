#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="drop table if exists eht";
${CLICKHOUSE_CLIENT} --query="create table eht(id UInt64, x UInt64) Engine=CnchMergeTree() order by x";
${CLICKHOUSE_CLIENT} --query="insert into eht select number, number from system.numbers limit 10000";
${CLICKHOUSE_CLIENT} --query="insert into eht select number + 10000, intDiv(number, 10) * 10 + 10000 from system.numbers limit 10000";
${CLICKHOUSE_CLIENT} --query="create stats eht with fullscan SETTINGS create_stats_time_output=0, statistics_histogram_bucket_size=200;";

${CLICKHOUSE_CLIENT} --query="show column_stats eht(x)" | awk -F'\t' '{ if ($4 >= 80 && $4 <= 120) print NR ": ok"; else print NR ": failed " $4; }'

${CLICKHOUSE_CLIENT} --query="drop table if exists eht";