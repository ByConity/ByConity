#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
. "$CURDIR"/setup_env.sh

set +x

HIVE_DATABASE="hive_testdb_$RANDOM"

hive-cli "create database if not exists ${HIVE_DATABASE}; \
create table if not exists ${HIVE_DATABASE}.t (user_id bigint,  page_url string,  country string) stored as parquet ;\
insert into ${HIVE_DATABASE}.t values (7,'http://example.com/page7','Australia') ;\
insert into ${HIVE_DATABASE}.t values (2,'http://example.com/page2','Canada') ; \
insert into ${HIVE_DATABASE}.t values (8,'http://example.com/page8','Brazil') ;\
insert into ${HIVE_DATABASE}.t values (4,'http://example.com/page4','Germany') ;\
insert into ${HIVE_DATABASE}.t values (3,'http://example.com/page3','UK')"

${CLICKHOUSE_CLIENT} --query "create table ce_t (user_id Int64, page_url String, country String) engine = CnchHive('${HIVE_METASTORE}', '${HIVE_DATABASE}', 't')"

${CLICKHOUSE_CLIENT} --query "select * from ce_t order by country"
${CLICKHOUSE_CLIENT} --query "select * from ce_t where country = 'Australia'"
