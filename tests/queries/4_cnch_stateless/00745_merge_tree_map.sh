#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map2"

${CLICKHOUSE_CLIENT} --query "SELECT 'test byte map type and kv map type'"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map1 (n UInt8, m1 Map(String, String), m2 Map(LowCardinality(String), LowCardinality(Nullable(String)))) Engine=CnchMergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map2 (n UInt8, m1 Map(String, String) KV, m2 Map(LowCardinality(String), LowCardinality(Nullable(String))) KV) Engine=CnchMergeTree ORDER BY n"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map1"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES test.00745_merge_tree_map2"

# write parts to test.00745_merge_tree_map1
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map1 VALUES (1, {'k1': 'v1'}, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map1"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map1 VALUES (2, {'k2': 'v2'}, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map1 ORDER BY n"

# write parts to test.00745_merge_tree_map2
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map2 VALUES (1, {'k1': 'v1'}, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map2"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map2 VALUES (2, {'k2': 'v2'}, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k1'}, m2{'k1'} FROM test.00745_merge_tree_map2 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k2'}, m2{'k2'} FROM test.00745_merge_tree_map1 ORDER BY n"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k2'}, m2{'k2'} FROM test.00745_merge_tree_map2 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map1 ADD COLUMN ma Map(String, Array(String))"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map2 ADD COLUMN ma Map(String, Array(String)) KV"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map1 VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k3'}, m2{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map1 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map2 VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m1{'k3'}, m2{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map2 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map1 DROP COLUMN ma"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map2 DROP COLUMN ma"
sleep 70 

${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map1"
${CLICKHOUSE_CLIENT} --query "SELECT n, arraySort(mapKeys(m1)), arraySort(mapKeys(m2)) FROM test.00745_merge_tree_map1 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map2"
${CLICKHOUSE_CLIENT} --query "SELECT n, arraySort(mapKeys(m1)), arraySort(mapKeys(m2)) FROM test.00745_merge_tree_map2 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map1 CLEAR MAP KEY m1('k2'), CLEAR MAP KEY m2('k2') SETTINGS mutations_sync=1"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map2 CLEAR MAP KEY m1('k2'), CLEAR MAP KEY m2('k2') SETTINGS mutations_sync=1" 2>&1 | grep -q "Code: 44." && echo "OK" || echo "Fail"
sleep 60

${CLICKHOUSE_CLIENT} --query "SELECT n, m1, m2 FROM test.00745_merge_tree_map1 ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map2"
