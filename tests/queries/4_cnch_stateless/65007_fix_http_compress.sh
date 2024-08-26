#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists test.httpcompress"
${CLICKHOUSE_CLIENT} --query "create table test.httpcompress(a int, b String, c Date) engine=CnchMergeTree() order by a"

curl -sS -d 'SELECT 1,2,3,42'  -X POST "${CLICKHOUSE_URL}&compress=1"
curl -sS -d 'SELECT 1,2,3,42'  -X POST "${CLICKHOUSE_URL}&compress=0"
curl -sS -d 'SELECT * except b from test.httpcompress where 0' -H 'x-clickhouse-format: RowBinaryWithNamesAndTypes'  -X POST "${CLICKHOUSE_URL}&compress=1&extremes=0"

${CLICKHOUSE_CLIENT} --query "drop table if exists test.httpcompress"
