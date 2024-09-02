#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# not useful on cnch
# catalog will automatically GC statistics

# ${CLICKHOUSE_CLIENT} --query="drop table if exists stats_ddl"
# ${CLICKHOUSE_CLIENT} --query="drop table if exists stats_ddl2"
# ${CLICKHOUSE_CLIENT} --query=""
# ${CLICKHOUSE_CLIENT} --query="CREATE TABLE stats_ddl(x UInt64, id UInt64) Engine=CnchMergeTree() order by id;"
# ${CLICKHOUSE_CLIENT} --query="
# ${CLICKHOUSE_CLIENT} --query="insert into stats_ddl select number * 10, number from system.numbers limit 2"

# echo "create stats"
# ${CLICKHOUSE_CLIENT} --query="create stats stats_ddl settings create_stats_time_output=0"
# echo "show stats1"
# ${CLICKHOUSE_CLIENT} --query="show stats stats_ddl"
# UUID=`${CLICKHOUSE_CLIENT} --query="select statistics_unique_key(database, name) from system.tables where name='stats_ddl' and database=currentDatabase()"`
# # echo ${UUID}
# ${CLICKHOUSE_CLIENT} --query="select count(*) from system.optimizer_statistics where table_uuid='${UUID}'"
# # TODO: support renaming under new version
# # echo "rename table"
# # ${CLICKHOUSE_CLIENT} --query="rename table stats_ddl to stats_ddl2"
# # ${CLICKHOUSE_CLIENT} --query="show stats stats_ddl2"
# # ${CLICKHOUSE_CLIENT} --query="select count(*) from system.optimizer_statistics where table_uuid='${UUID}'"

# echo "drop table"
# ${CLICKHOUSE_CLIENT} --query="drop table stats_ddl SYNC"
# ${CLICKHOUSE_CLIENT} --query="select count(*) from system.optimizer_statistics where table_uuid='${UUID}'"


# ${CLICKHOUSE_CLIENT} --query="drop table if exists stats_ddl"
# ${CLICKHOUSE_CLIENT} --query=""

