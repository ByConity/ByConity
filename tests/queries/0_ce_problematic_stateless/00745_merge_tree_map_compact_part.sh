#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh
ch_dir=`${CLICKHOUSE_EXTRACT_CONFIG} -k path`

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_map_compact_part"

### test compact format and uncompact map type
${CLICKHOUSE_CLIENT} --query "SELECT 'test compact format and uncompact map type'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_compact_part (n UInt8, m Map(String, String)) Engine=MergeTree ORDER BY n settings min_bytes_for_compact_part = 0, enable_compact_map_data = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (1, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part"
${CLICKHOUSE_CLIENT} --query "SELECT name, part_type, compact_map FROM system.parts WHERE database = 'test' and table = '00745_merge_tree_map_compact_part' and active"

test -e $ch_dir/data/test/00745_merge_tree_map_compact_part/all_*/__m__%27k1%27.bin && echo "map implicit column file exist"
test -e $ch_dir/data/test/00745_merge_tree_map_compact_part/all_*/m.bin && echo "map implicit column file exist"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (2, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_compact_part"
sleep 2
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k2'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part ADD COLUMN ma Map(String, Array(String))"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part DROP COLUMN ma"

${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map_compact_part"

${CLICKHOUSE_CLIENT} --query "SELECT n, mapKeys(m) FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part CLEAR MAP KEY m('k2')"

${CLICKHOUSE_CLIENT} --query "SELECT n, m FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map_compact_part"

### test compact format and compact map type
${CLICKHOUSE_CLIENT} --query "SELECT ''"
${CLICKHOUSE_CLIENT} --query "SELECT 'test compact format and compact map type'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_compact_part (n UInt8, m Map(String, String)) Engine=MergeTree ORDER BY n settings min_bytes_for_compact_part = 0, enable_compact_map_data = 1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (1, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part"
${CLICKHOUSE_CLIENT} --query "SELECT name, part_type, compact_map FROM system.parts WHERE database = 'test' and table = '00745_merge_tree_map_compact_part' and active"

test -e $ch_dir/data/test/00745_merge_tree_map_compact_part/all_*/__m__%27k1%27.bin && echo "map implicit column file exist"
test -e $ch_dir/data/test/00745_merge_tree_map_compact_part/all_*/m.bin && echo "map file exist"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (2, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_compact_part"
sleep 2
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k2'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part ADD COLUMN ma Map(String, Array(String))"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part DROP COLUMN ma"

${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map_compact_part"

${CLICKHOUSE_CLIENT} --query "SELECT n, mapKeys(m) FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part CLEAR MAP KEY m('k2')"
${CLICKHOUSE_CLIENT} --query "SELECT n, m FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map_compact_part"

### test compact format and KV map type
${CLICKHOUSE_CLIENT} --query "SELECT ''"
${CLICKHOUSE_CLIENT} --query "SELECT 'test compact format and KV map type'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_map_compact_part (n UInt8, m Map(String, String) KV) Engine=MergeTree ORDER BY n settings min_bytes_for_compact_part = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (1, {'k1': 'v1'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part"

test -e $ch_dir/data/test/00745_merge_tree_map_compact_part/all_*/m%2Ekey.bin && echo "map file exist" || echo "map file not exist"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (2, {'k2': 'v2'})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k1'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test.00745_merge_tree_map_compact_part"
sleep 2
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k2'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part ADD COLUMN ma Map(String, Array(String)) KV"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.00745_merge_tree_map_compact_part VALUES (3, {'k3': 'v3', 'k3.1': 'v3.1'}, {'k3': ['v3.1', 'v3.2']})"
${CLICKHOUSE_CLIENT} --query "SELECT n, m{'k3'}, ma{'k3'} FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part DROP COLUMN ma"
${CLICKHOUSE_CLIENT} --query "DESC TABLE test.00745_merge_tree_map_compact_part"

${CLICKHOUSE_CLIENT} --query "SELECT n, mapKeys(m) FROM test.00745_merge_tree_map_compact_part ORDER BY n"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.00745_merge_tree_map_compact_part CLEAR MAP KEY m('k2')" 2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_map_compact_part"
