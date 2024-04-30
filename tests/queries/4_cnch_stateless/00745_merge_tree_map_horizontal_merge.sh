#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge2;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge3;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge4;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge5;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge6;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 00745_merge_tree_map_merge7;"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE 00745_merge_tree_map_merge1 (
    n                   UInt8,
    string_map          Map(String, String) KV,
    string_array_map    Map(String, Array(String)) KV,
    fixed_string_map    Map(FixedString(2), FixedString(2)) KV,
    int_map             Map(UInt32, UInt32) KV,
    lowcardinality_map  Map(LowCardinality(String), LowCardinality(String)) KV,
    float_map           Map(Float32, Float32) KV,
    date_map            Map(Date, Date) KV,
    datetime_map        Map(DateTime('Asia/Shanghai'), DateTime('Asia/Shanghai')) KV,
    uuid_map            Map(UUID, UUID) KV,
    enums_map           Map(Enum8('hello' = 0, 'world' = 1, 'foo' = -1), Enum8('hello' = 0, 'world' = 1, 'foo' = -1)) KV,
    nullable_map        Map(String, Nullable(Int32)) KV
) ENGINE = CnchMergeTree ORDER BY n
SETTINGS enable_vertical_merge_algorithm = 0;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE 00745_merge_tree_map_merge2 (
    n                   UInt8,
    string_map          Map(String, String),
    string_array_map    Map(String, Array(String)),
    fixed_string_map    Map(FixedString(2), FixedString(2)),
    int_map             Map(UInt32, UInt32),
    lowcardinality_map  Map(LowCardinality(String), LowCardinality(Nullable(String))),
    float_map           Map(Float32, Float32),
    date_map            Map(Date, Date),
    datetime_map        Map(DateTime('Asia/Shanghai'), DateTime('Asia/Shanghai')),
    uuid_map            Map(UUID, Int32),
    enums_map           Map(Enum8('hello' = 0, 'world' = 1, 'foo' = -1), Enum8('hello' = 0, 'world' = 1, 'foo' = -1))
) ENGINE = CnchMergeTree ORDER BY n
SETTINGS enable_vertical_merge_algorithm = 0;
EOF

${CLICKHOUSE_CLIENT} --query "CREATE TABLE 00745_merge_tree_map_merge3 (n UInt8, \`m[a]\` Map(String, String), \`m{a}\` Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS enable_vertical_merge_algorithm = 0;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE 00745_merge_tree_map_merge4 (n UInt8, \`m.a\` Map(String, String), \`m.b\` Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple();"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE 00745_merge_tree_map_merge5 (empty_map Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS enable_vertical_merge_algorithm = 0;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE 00745_merge_tree_map_merge6 (string_map Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple() SETTINGS enable_vertical_merge_algorithm = 0;"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge1;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge2;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge3;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge4;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge5;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES 00745_merge_tree_map_merge6;"

for i in {0..3}
do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge1 VALUES ($i, {'key1': 'v1'}, {'key1': ['v1']}, {'k1': 'v1'}, {1: 1}, {'key1': 'v1'}, {1.0: 1.0}, {'2023-10-01': '2023-10-01'}, {'2023-10-01 10:00:00': '2023-10-01 10:00:00'}, {'ae6bc284-7ead-48b8-a7e3-8477f9a1aa78': 'ae6bc284-7ead-48b8-a7e3-8477f9a1aa78'}, {'hello': 'world'}, {'key1': 1});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge2 VALUES ($i, {'key1': 'v1'}, {'key1': ['v1']}, {'k1': 'v1'}, {1: 1}, {'key1': 'v1'}, {1.0: 1.0}, {'2023-10-01': '2023-10-01'}, {'2023-10-01 10:00:00': '2023-10-01 10:00:00'}, {'ae6bc284-7ead-48b8-a7e3-8477f9a1aa78': 1}, {'hello': 'world'});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge3 VALUES ($i, {'key1[1]': 'v1[1]'}, {'key1{1}': 'v1{1}'});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge4 VALUES ($i, {'key1.1': 'v1.1'}, {'key1.1': 'v1.1'});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge5 VALUES ({});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge6 VALUES ({'key1': 'v1'});"
done

${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge1 VALUES (4, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge2 VALUES (4, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge3 VALUES (4, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge4 VALUES (4, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge5 VALUES ({});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO 00745_merge_tree_map_merge6 VALUES ({});"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
    SET disable_optimize_final = 0;
    OPTIMIZE TABLE 00745_merge_tree_map_merge1 FINAL SETTINGS mutations_sync=1;
    OPTIMIZE TABLE 00745_merge_tree_map_merge2 FINAL SETTINGS mutations_sync=1;
    OPTIMIZE TABLE 00745_merge_tree_map_merge3 FINAL SETTINGS mutations_sync=1;
    OPTIMIZE TABLE 00745_merge_tree_map_merge4 FINAL SETTINGS mutations_sync=1;
    OPTIMIZE TABLE 00745_merge_tree_map_merge5 FINAL SETTINGS mutations_sync=1;
    OPTIMIZE TABLE 00745_merge_tree_map_merge6 FINAL SETTINGS mutations_sync=1;
EOF
# optimization maybe unfinished even if add mutations_sync=1, sleep here is slow but can make ci stable
sleep 80

${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge1' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge2' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge3' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge4' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge5' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = currentDatabase(1) AND table = '00745_merge_tree_map_merge6' AND active;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge1 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge2 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge3 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge4 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge5 ORDER BY empty_map;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM 00745_merge_tree_map_merge6 ORDER BY string_map;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge2;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge3;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge4;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge5;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 00745_merge_tree_map_merge6;"
