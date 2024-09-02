#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# TMP_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT/1234/345}"
# TMP_CLICKHOUSE_CLIENT=$(echo "$TMP_CLICKHOUSE_CLIENT" | sed 's/--database=[^[:space:]]*//')
TMP_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --tenant_id='345'"
${TMP_CLICKHOUSE_CLIENT}  --query "create database if not exists db1"
${TMP_CLICKHOUSE_CLIENT}  --query "create table if not exists db1.tb(x int, y int) engine=CnchMergeTree() order by x"
${TMP_CLICKHOUSE_CLIENT}  --query "insert into db1.tb select number, 2 * number from numbers(10)"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, name, sorting_key from system.cnch_tables where database = 'db1'"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, name, sorting_key from system.cnch_tables where database = 'db1' and name = 'tb'"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, table, name from system.cnch_columns where database = 'db1'"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, table, name from system.cnch_columns where database = 'db1' and table = 'tb'"
${TMP_CLICKHOUSE_CLIENT}  --query "select distinct database, table from system.cnch_parts where database = 'db1' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"
${TMP_CLICKHOUSE_CLIENT}  --query "select distinct database, table from system.cnch_parts where database = 'db1' and table = 'tb' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"


# TMP_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT/1234/456}"
# TMP_CLICKHOUSE_CLIENT=$(echo "$TMP_CLICKHOUSE_CLIENT" | sed 's/--database=[^[:space:]]*//')
TMP_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --tenant_id='456'"
${TMP_CLICKHOUSE_CLIENT}  --query "create database if not exists db1"
${TMP_CLICKHOUSE_CLIENT}  --query "create table if not exists db1.ta(p int, q int) engine=CnchMergeTree() order by p"
${TMP_CLICKHOUSE_CLIENT}  --query "insert into db1.ta select number, 2 * number from numbers(10)"

${TMP_CLICKHOUSE_CLIENT}  --query "select database, name, sorting_key from system.cnch_tables"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, name, sorting_key from system.cnch_tables where database = 'db1'"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, name, sorting_key from system.cnch_tables where database = 'db1' and name = 'ta'"

${TMP_CLICKHOUSE_CLIENT}  --query "select database, table, name from system.cnch_columns"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, table, name from system.cnch_columns where database = 'db1'"
${TMP_CLICKHOUSE_CLIENT}  --query "select database, table, name from system.cnch_columns where database = 'db1' and table = 'ta'"

${TMP_CLICKHOUSE_CLIENT}  --query "select distinct database, table from system.cnch_parts where visible=1 settings enable_multiple_tables_for_cnch_parts=1"
${TMP_CLICKHOUSE_CLIENT}  --query "select distinct database, table from system.cnch_parts where database = 'db1' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"
${TMP_CLICKHOUSE_CLIENT}  --query "select distinct database, table from system.cnch_parts where database = 'db1' and table = 'ta' and visible=1 settings enable_multiple_tables_for_cnch_parts=1"

#${CLICKHOUSE_CLIENT} --tenant_id='456' -mn --query "truncate db1.tb;  -- drop db1; "
#${CLICKHOUSE_CLIENT} --tenant_id='345' -mn --query "truncate db1.ta;  -- drop db1; "
