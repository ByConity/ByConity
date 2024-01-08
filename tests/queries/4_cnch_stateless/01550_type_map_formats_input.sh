#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS map_formats_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE map_formats_input (a Int32, m Map(String, UInt32), m1 Map(String, Date), m2 Map(String, Array(UInt32))) ENGINE = CnchMergeTree order by a;" --allow_experimental_map_type 1

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT JSONEachRow" <<< '{"a":0,"m":{"k1":1,"k2":2,"k3":3},"m1":{"k1":"2020-05-05"},"m2":{"k1":[],"k2":[7,8]}}'
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT CSV" <<< "\"0\",\"{'k1':1,'k2':2,'k3':3}\",\"{'k1':'2020-05-05'}\",\"{'k1':[],'k2':[7,8]}\""
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT TSV" <<< "0	{'k1':1,'k2':2,'k3':3}	{'k1':'2020-05-05'}	{'k1':[],'k2':[7,8]}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q 'SELECT * FROM map_formats_input FORMAT Native' | $CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q "DROP TABLE map_formats_input"

$CLICKHOUSE_CLIENT -q "CREATE TABLE map_formats_input (a Int32, m Map(String, String)) ENGINE = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT --testmode -q "INSERT INTO map_formats_input FORMAT JSONEachRow SETTINGS input_format_allow_errors_num = 1" <<EOF
    {"a": 0, "m": {"key1": "v1", "key2": "v2", "key3": "v3"}}
    {"a": 1, "m": {"key1": "v1", "key2": "\ud83d$##$1", "key3": "v3"}}
    {"a": 2, "m": {"key1": "v1", "key2": "v2", "key3": "v3"}}
EOF

$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input ORDER BY a;"
$CLICKHOUSE_CLIENT -q "DROP TABLE map_formats_input;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE array_formats_input (a Int32, arr Array(String)) ENGINE = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT --testmode -q "INSERT INTO array_formats_input FORMAT JSONEachRow SETTINGS input_format_allow_errors_num = 1" <<EOF
    {"a": 0, "arr": ["e1", "e2", "e3"]}
    {"a": 1, "arr": ["e1", "\ud83d$##$1", "e3"]}
    {"a": 2, "arr": ["e1", "e2", "e3"]}
EOF
$CLICKHOUSE_CLIENT -q "SELECT * FROM array_formats_input ORDER BY a;"
$CLICKHOUSE_CLIENT -q "DROP TABLE array_formats_input;"

$CLICKHOUSE_CLIENT -q "CREATE TABLE tuple_formats_input (a Int32, t Tuple(String, String, String)) ENGINE = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT --testmode -q "INSERT INTO tuple_formats_input FORMAT JSONEachRow SETTINGS input_format_allow_errors_num = 1" <<EOF
    {"a": 0, "t": ["e1", "e2", "e3"]}
    {"a": 1, "t": ["e1", "\ud83d$##$1", "e3"]}
    {"a": 2, "t": ["e1", "e2", "e3"]}
EOF
$CLICKHOUSE_CLIENT -q "SELECT * FROM tuple_formats_input ORDER BY a;"
$CLICKHOUSE_CLIENT -q "DROP TABLE tuple_formats_input;"


$CLICKHOUSE_CLIENT -q "CREATE TABLE tuple_formats_input (a Int32, t Tuple(key1 String, key2 String, key3 String)) ENGINE = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT --testmode -q "INSERT INTO tuple_formats_input FORMAT JSONEachRow SETTINGS input_format_allow_errors_num = 1, output_format_json_named_tuples_as_objects = 1" <<EOF
    {"a": 0, "t": {"key1": "v1", "key2": "v2", "key3": "v3"}}
    {"a": 1, "t": {"key1": "v1", "key2": "\ud83d$##$1", "key3": "v3"}}
    {"a": 2, "t": {"key1": "v1", "key2": "v2", "key3": "v3"}}
EOF
$CLICKHOUSE_CLIENT -q "SELECT * FROM tuple_formats_input ORDER BY a;"
$CLICKHOUSE_CLIENT -q "DROP TABLE tuple_formats_input;"
