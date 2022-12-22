#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh
ch_dir=`${CLICKHOUSE_EXTRACT_CONFIG} -k path`

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.00745_merge_tree_check_column_vadility"

### test invalid column name who is map implicit key
${CLICKHOUSE_CLIENT} --query "SELECT 'test invalid column name who is map implicit key'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`__a\` String) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a.key\` String) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a.value\` String) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"

## test invalid column name whose type is map
${CLICKHOUSE_CLIENT} --query "SELECT ''"
${CLICKHOUSE_CLIENT} --query "SELECT 'test invalid column name whose type is map'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a__a\` Map(String, String)) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a_a_\` Map(String, String)) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a_a_\` Map(String, LowCardinality(Nullable(String)))) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 1"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"

## test invalid map key or value type
${CLICKHOUSE_CLIENT} --query "SELECT ''"
${CLICKHOUSE_CLIENT} --query "SELECT 'test invalid map key or value type'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, a Map(String, Tuple(String, String))) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 36," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, a Map(Nullable(String), String)) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 36," && echo "OK" || echo "Fail"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, a Map(String, LowCardinality(String))) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 36," && echo "OK" || echo "Fail"

## test create column whose name is same with func column name
${CLICKHOUSE_CLIENT} --query "SELECT ''"
${CLICKHOUSE_CLIENT} --query "SELECT 'test create column whose name is same with func column name'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`_delete_flag_\` Map(String, String)) Engine=HaUniqueMergeTree('/clickhouse/test/00745_merge_tree_check_column_vadility', '1') ORDER BY n UNIQUE KEY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "OK" || echo "Fail"

## right column name
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.00745_merge_tree_check_column_vadility (n UInt8, \`a_a\` Map(String, String), \`_a_a\` Map(String, String)) Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, enable_compact_map_data = 0"  2>&1 | grep -q "Code: 44," && echo "FAIL" || echo "OK"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test.00745_merge_tree_check_column_vadility"
