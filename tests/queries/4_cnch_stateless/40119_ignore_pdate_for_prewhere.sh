#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query="create database if not exists test"
$CLICKHOUSE_CLIENT --query="drop table if exists test.40119_ignore_pdate_prewhere"
$CLICKHOUSE_CLIENT --query="create table test.40119_ignore_pdate_prewhere (p_date Date, i Int32, s String) engine = CnchMergeTree() partition by p_date order by i"
$CLICKHOUSE_CLIENT --query="insert into test.40119_ignore_pdate_prewhere select '2024-09-01' as p_date, number as i, toString(10000000000 + i) as s from system.numbers limit 1000"

uuid=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 6; echo)

$CLICKHOUSE_CLIENT --query_id "40119_ignore_pdate_prewhere_${uuid}" --query="select count() from test.40119_ignore_pdate_prewhere where p_date = '2024-09-01' and s = '10000000100' settings enable_optimizer = 1 FORMAT Null"

sleep 40

$CLICKHOUSE_CLIENT --query="SELECT sum(ProfileEvents{'PrewhereSelectedRows'}) FROM cnch(worker, system.query_log) WHERE initial_query_id = '40119_ignore_pdate_prewhere_${uuid}'"

$CLICKHOUSE_CLIENT --query="drop table if exists test.40119_ignore_pdate_prewhere"
