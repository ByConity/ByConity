#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="drop table if exists db40115.t40115"

$CLICKHOUSE_CLIENT --query="create database if not exists db40115"
$CLICKHOUSE_CLIENT --query="create table db40115.t40115(p_date Date, int_params Map(String, Int64)) engine = CnchMergeTree() partition by p_date order by tuple()"
$CLICKHOUSE_CLIENT --query="insert into db40115.t40115 select addDays('2024-01-01', number / 1000) as p_date, map(randomString(10), 1) from system.numbers limit 100000 settings max_partitions_per_insert_block=100"

# warmup
$CLICKHOUSE_CLIENT --query="select getMapKeys('db40115', 't40115', 'int_params') format Null" &>/dev/null

time_in_non_opt=$($CLICKHOUSE_CLIENT --query="select getMapKeys('db40115', 't40115', 'int_params') format Null settings enable_optimizer=0" --time 2>&1 >/dev/null)
time_in_opt=$($CLICKHOUSE_CLIENT --query="select getMapKeys('db40115', 't40115', 'int_params') format Null settings enable_optimizer=1" --time 2>&1 >/dev/null)

# give max(0.2, 5%) buffer for performance jitter
$CLICKHOUSE_CLIENT --query="SELECT $time_in_opt < $time_in_non_opt + 0.2 OR $time_in_opt < $time_in_non_opt * 1.05"

$CLICKHOUSE_CLIENT --query="drop table if exists db40115.t40115"
