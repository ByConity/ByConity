#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

CLICKHOUSE_CLIENT0="$CLICKHOUSE_CLIENT_BINARY ${CLICKHOUSE_CLIENT_OPT0:-}"

${CLICKHOUSE_CLIENT0} --query="DROP DATABASE IF EXISTS dump"
${CLICKHOUSE_CLIENT0} --query="CREATE DATABASE IF NOT EXISTS dump"
${CLICKHOUSE_CLIENT0} --query="DROP TABLE IF EXISTS dump.dumpclient"
${CLICKHOUSE_CLIENT0} --query="DROP TABLE IF EXISTS dump.dumpclient_local"


${CLICKHOUSE_CLIENT0} --query="CREATE TABLE dump.dumpclient_local(a UInt8, b String) ENGINE = MergeTree() PARTITION BY a PRIMARY KEY a ORDER BY a"
${CLICKHOUSE_CLIENT0} --query="create table dump.dumpclient as dump.dumpclient_local engine = Distributed(test_shard_localhost, dump, dumpclient_local)"
${CLICKHOUSE_CLIENT0} --query="insert into dump.dumpclient values(1,'a')(2,'d')(3,'c')"


${CLICKHOUSE_CLIENT0} --query="dump ddl from dump"
# codebase have bugs which may change the settings of global context, leading to an unstable output for dump query
${CLICKHOUSE_CLIENT0} --query_id="dump-test" --query="dump query select * from dump.dumpclient" | grep -F -q '"query" : "SELECT * FROM dump.dumpclient"' && echo 'OK' || echo 'FAIL'

# stats dump contains a timestamp which varies for each test run
# ${CLICKHOUSE_CLIENT0} --query="create stats dumpclient"
# ${CLICKHOUSE_CLIENT0} --query="dump ddl from dump"

${CLICKHOUSE_CLIENT0} --query="DROP TABLE IF EXISTS dump.dumpclient"
${CLICKHOUSE_CLIENT0} --query="DROP TABLE IF EXISTS dump.dumpclient_local"
