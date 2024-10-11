#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
. "$CURDIR"/setup_env.sh

hive-cli "create database if not exists ${CLICKHOUSE_DATABASE}; \
create table if not exists ${CLICKHOUSE_DATABASE}.t00011 (s1 string , s2 string, arr1 array<bigint>) stored as parquet ;"

${CLICKHOUSE_CLIENT} --query "create table if not exists hive_t00011 (s1 String, s2 String, arr1 Array(Int64)) engine = CnchHive('${HIVE_METASTORE}', '${CLICKHOUSE_DATABASE}', 't00011')"
( ${CLICKHOUSE_CLIENT} --query "explain select (lower(upper(substring(s1 || s2, 2)))) as a, count() from hive_t00011 prewhere length(a) > 10 group by a settings enable_optimizer=1, enable_common_expression_sharing = 1, enable_common_expression_sharing_for_prewhere = 1" | grep -ie 'Inline expression' ) && echo "fail"

exit 0
