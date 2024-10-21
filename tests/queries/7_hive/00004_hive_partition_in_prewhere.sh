#!/usr/bin/env bash

#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
. "$CURDIR"/setup_env.sh

set +x

HIVE_DATABASE="hive_testdb_$RANDOM"

hive-cli "create database if not exists ${HIVE_DATABASE}"
hive-cli "create table if not exists ${HIVE_DATABASE}.par_t (s String) partitioned by (p String) stored as parquet"
hive-cli "insert into ${HIVE_DATABASE}.par_t partition(p = 'p1') values ('abc')"
hive-cli "insert into ${HIVE_DATABASE}.par_t partition(p = 'p2') values ('def')"

${CLICKHOUSE_CLIENT} --query "create table par_t (s String, p String) engine = HiveCluster('${HIVE_CLUSTER_NAME}', '${HIVE_METASTORE}', '${HIVE_DATABASE}', 'par_t') partition by p"

${CLICKHOUSE_CLIENT} --query "select s,p from par_t PREWHERE p='p1' SETTINGS enable_optimizer=1, hive_use_native_reader=1, allow_experimental_datalake_unified_engine=1"
${CLICKHOUSE_CLIENT} --query "select s,p from par_t PREWHERE p='p1' SETTINGS enable_optimizer=1, hive_use_native_reader=0, allow_experimental_datalake_unified_engine=1"

hive-cli "create database if not exists ${HIVE_DATABASE}"
hive-cli "create table if not exists ${HIVE_DATABASE}.orc_t (s String) partitioned by (p String) stored as orc"
hive-cli "insert into ${HIVE_DATABASE}.orc_t partition(p = 'p1') values ('abc')"
hive-cli "insert into ${HIVE_DATABASE}.orc_t partition(p = 'p2') values ('def')"

${CLICKHOUSE_CLIENT} --query "create table orc_t (s String, p String) engine = HiveCluster('${HIVE_CLUSTER_NAME}', '${HIVE_METASTORE}', '${HIVE_DATABASE}', 'orc_t') partition by p"

${CLICKHOUSE_CLIENT} --query "select s,p from orc_t PREWHERE p='p1' SETTINGS enable_optimizer=1, hive_use_native_reader=1, allow_experimental_datalake_unified_engine=1"
${CLICKHOUSE_CLIENT} --query "select s,p from orc_t PREWHERE p='p1' SETTINGS enable_optimizer=1, hive_use_native_reader=0, allow_experimental_datalake_unified_engine=1"

