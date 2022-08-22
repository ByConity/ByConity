#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS table;

CREATE TABLE table (id Int64,
                         address Nullable(String),
                         name String,
                         nums Array(UInt32),
                         string_list Map(String, Array(String)),
                         params_int Map(Int32, Int32)
                         ) ENGINE = CnchMergeTree ORDER BY tuple();
EOF

cat $CURDIR/00825_protobuf_custom_format_input.data | $CLICKHOUSE_CLIENT --query="INSERT INTO table FORMAT Protobuf SETTINGS format_schema = '$CURDIR/00825_protobuf_custom_format:TestMessage'"

$CLICKHOUSE_CLIENT --query "SELECT * FROM table;"