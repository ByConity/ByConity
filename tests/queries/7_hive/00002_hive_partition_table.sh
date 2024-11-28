#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
. "$CURDIR"/setup_env.sh

set +x

HIVE_DATABASE="hive_testdb_$RANDOM"

hive-cli "create database if not exists ${HIVE_DATABASE}; \
create table if not exists ${HIVE_DATABASE}.t (  user_id bigint, page_url string ) partitioned by (country String) stored as parquet; \
insert into ${HIVE_DATABASE}.t partition(country = 'Australia') values (7,'http://example.com/page7'); \
insert into ${HIVE_DATABASE}.t partition(country = 'Canada') values (2,'http://example.com/page2'); \
insert into ${HIVE_DATABASE}.t partition(country = 'Brazil') values (8,'http://example.com/page8'); \
insert into ${HIVE_DATABASE}.t partition(country = 'Germany') values (4,'http://example.com/page4'); \
insert into ${HIVE_DATABASE}.t partition(country = 'UK') values (3,'http://example.com/page3')"

${CLICKHOUSE_CLIENT} --query "create table ce_t (user_id Int64, page_url String, country String) engine = CnchHive('${HIVE_METASTORE}', '${HIVE_DATABASE}', 't') partition by country"

${CLICKHOUSE_CLIENT} --query "select user_id, page_url, country as country from ce_t order by country settings enable_optimizer = 0 "
${CLICKHOUSE_CLIENT} --query "select user_id, page_url, country as country from ce_t where country in ('Australia', 'NonExist') settings enable_optimizer = 1 "
${CLICKHOUSE_CLIENT} --query "select user_id, page_url, country as country from ce_t where country = 'Australia' settings enable_optimizer = 1"
