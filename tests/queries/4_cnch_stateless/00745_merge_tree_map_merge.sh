#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_merge1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_merge2;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_merge3;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_merge4;"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE test.00745_merge_tree_map_merge1 (
    n                   UInt8,
    string_map          Map(String, String) KV,
    string_array_map    Map(String, Array(String)) KV,
    int_map             Map(UInt32, UInt32) KV,
    int_array_map       Map(UInt32, Array(UInt32)) KV,
    float_map           Map(String, Float64) KV,
    float_array_map     Map(String, Array(Float64)) KV
) ENGINE = CnchMergeTree ORDER BY n;
EOF

${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE test.00745_merge_tree_map_merge2 (
    n                   UInt8,
    string_map          Map(String, String),
    string_array_map    Map(String, Array(String)),
    int_map             Map(UInt32, UInt32),
    int_array_map       Map(UInt32, Array(UInt32)),
    float_map           Map(String, Float64),
    float_array_map     Map(String, Array(Float64))
) ENGINE = CnchMergeTree ORDER BY n;
EOF

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_merge3 (n UInt8, string_map Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple();"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_merge4 (n UInt8, empty_map Map(String, String)) ENGINE = CnchMergeTree ORDER BY tuple();"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map_merge1;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map_merge2;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map_merge3;"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map_merge4;"

for i in {0..3}
do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge1 VALUES ($i, {'key$i': 'v1'}, {'key$i': ['v1']},  {$i: 1}, {$i: [1]}, {'key$i': 0.1}, {'key$i': [0.1]});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge2 VALUES ($i, {'key$i': 'v1'}, {'key$i': ['v1']},  {$i: 1}, {$i: [1]}, {'key$i': 0.1}, {'key$i': [0.1]});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge3 VALUES ($i, {'key1': 'v1'});"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge4 VALUES ($i, {});"
done

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge1 VALUES (4, {}, {}, {}, {}, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge2 VALUES (4, {}, {}, {}, {}, {}, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge3 VALUES (4, {});"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_merge4 VALUES (4, {});"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_merge1 settings mutations_sync = 1;"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_merge2 settings mutations_sync = 1;"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_merge3 settings mutations_sync = 1;"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_merge4 settings mutations_sync = 1;"
sleep 80

${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = '${TENANT_DB_PREFIX}test' AND table = '00745_merge_tree_map_merge1' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = '${TENANT_DB_PREFIX}test' AND table = '00745_merge_tree_map_merge2' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = '${TENANT_DB_PREFIX}test' AND table = '00745_merge_tree_map_merge3' AND active;"
${CLICKHOUSE_CLIENT} --query "SELECT count(1) FROM system.cnch_parts WHERE database = '${TENANT_DB_PREFIX}test' AND table = '00745_merge_tree_map_merge4' AND active;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.00745_merge_tree_map_merge1 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.00745_merge_tree_map_merge2 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.00745_merge_tree_map_merge3 ORDER BY n;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.00745_merge_tree_map_merge4 ORDER BY n;"