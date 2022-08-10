#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

JSON_FILE=$CURDIR/data_json/test_data.json

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS inject_test;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE inject_test(id UInt32, class String, students Array(String)) ENGINE=MergeTree PARTITION BY id ORDER BY tuple();"
${CLICKHOUSE_CLIENT} --query="INSERT INTO inject_test FORMAT JSONEachRow INFILE '$JSON_FILE';"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM inject_test ORDER BY class;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE inject_test;"
