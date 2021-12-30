#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh
ch_dir=`${CLICKHOUSE_EXTRACT_CONFIG} -k path`

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_lc"

### test wide format and uncompact map type
${CLICKHOUSE_CLIENT} --query "SELECT 'test wide format and uncompact map type'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_lc (n UInt8, m Map(String, LowCardinality(Nullable(String)))) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_lc VALUES (1, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_lc"

test -e $ch_dir/data/test/00745_merge_tree_map_lc/all_*/__m__%27k1%27.bin && echo "map implicit column file exist"
test -e $ch_dir/data/test/00745_merge_tree_map_lc/all_*/m.bin && echo "map implicit column file exist"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_lc VALUES (2, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_lc ORDER BY n"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_lc"
sleep 2
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k2'} FROM test.00745_merge_tree_map_lc ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_lc ADD COLUMN ma Map(String, Array(String))"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_lc VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map_lc ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_lc DROP COLUMN ma"
${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map_lc"

${CLICKHOUSE_CLIENT} --query "SELECT n, mapKeys(m) FROM test.00745_merge_tree_map_lc ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_lc CLEAR MAP KEY m('k2')"
${CLICKHOUSE_CLIENT} --query "SELECT n, m FROM test.00745_merge_tree_map_lc ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map_lc"
