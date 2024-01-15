#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CURDIR/../shell_config.sh

DATA_DIR=$CURDIR/data_avro

$CLICKHOUSE_CLIENT --multiquery <<'EOF'
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.test_insert_primitive_avro;
DROP TABLE IF EXISTS test.test_insert_complex_avro;
DROP TABLE IF EXISTS test.test_insert_nested_avro;
DROP TABLE IF EXISTS test.test_insert_map_avro;
EOF

# primitive types
echo '===test primitive types==='

$CLICKHOUSE_CLIENT --query "CREATE TABLE test.test_insert_primitive_avro \
( \
    a_bool UInt8,     \
    b_int Int32,      \
    c_long Int64,     \
    d_float Float32,  \
    e_double Float64, \
    f_bytes String,   \
    g_string String   \
) \
ENGINE = CnchMergeTree PARTITION BY tuple() ORDER BY b_int"

cat "$DATA_DIR"/primitive.avro | $CLICKHOUSE_CLIENT --query "INSERT INTO test.test_insert_primitive_avro FORMAT Avro"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test.test_insert_primitive_avro"

# complex types
echo '===test complex types==='

$CLICKHOUSE_CLIENT --query "CREATE TABLE test.test_insert_complex_avro \
( \
    a_enum_to_string String, \
    b_enum_to_enum Enum('t' = 1, 'f' = 0), \
    c_array_string Array(String), \
    d_array_array_string Array(Array(String)), \
    e_union_null_string Nullable(String), \
    f_union_long_null Nullable(Int64), \
    g_fixed FixedString(32) \
) \
ENGINE = CnchMergeTree \
PARTITION BY tuple() \
ORDER BY b_enum_to_enum"

cat "$DATA_DIR"/complex.avro | $CLICKHOUSE_CLIENT --query "INSERT INTO test.test_insert_complex_avro FORMAT Avro"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test.test_insert_complex_avro"

# nested types
echo '===test nested types==='

$CLICKHOUSE_CLIENT --query "CREATE TABLE test.test_insert_nested_avro \
( \
    a Int64, \
    \`b.a\` String, \
    \`b.b\` Double, \
    \`b.c\` Double, \
    c String \
) \
ENGINE = CnchMergeTree \
PARTITION BY tuple() \
ORDER BY a" \

cat "$DATA_DIR"/nested.avro | $CLICKHOUSE_CLIENT --query "INSERT INTO test.test_insert_nested_avro FORMAT Avro"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test.test_insert_nested_avro"

# test map
echo '===test map type==='
$CLICKHOUSE_CLIENT --query "CREATE TABLE test.test_insert_map_avro \
( \
    id Int64, \
    str_map Map(String, String), \
    int_map Map(String, Int64) \
) \
ENGINE = CnchMergeTree \
PARTITION BY tuple() \
ORDER BY id"

cat "$DATA_DIR"/test_map.avro | $CLICKHOUSE_CLIENT --query "INSERT INTO test.test_insert_map_avro FORMAT Avro"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test.test_insert_map_avro"

# TODO: test output format

$CLICKHOUSE_CLIENT --query "DROP TABLE test.test_insert_primitive_avro"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.test_insert_complex_avro"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.test_insert_nested_avro"
$CLICKHOUSE_CLIENT --query "DROP TABLE test.test_insert_map_avro"