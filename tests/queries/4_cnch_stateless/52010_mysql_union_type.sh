#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function wrapSQL() {
  sql=$1 
  echo "SELECT * FROM (${sql}) ORDER BY val"
}

# UInt64 UNION Int64
echo "UInt64 vs Int64"
for a in 0 9223372036854775807 18446744073709551615
do 
  for b in -9223372036854775808 9223372036854775807
  do
    echo $a, $b
    sql=$(wrapSQL "SELECT cast(${a} AS BIGINT UNSIGNED) AS val UNION SELECT cast(${b} AS BIGINT SIGNED) AS val")
    ${CLICKHOUSE_CLIENT} --dialect_type='MYSQL' --enable_implicit_arg_type_convert=1 --query="${sql}"
    sql="SELECT toTypeName(val) from (${sql})"
    ${CLICKHOUSE_CLIENT} --dialect_type='MYSQL' --enable_implicit_arg_type_convert=1 --query="${sql}"
  done
done


# UInt64/Int64 UNION Float/Double
echo "UInt64/Int64 UNION Float/Double"
for a in "BIGINT UNSIGNED" "BIGINT SIGNED"
do 
  for b in "Float" "Double"
  do
    echo $a, $b
    sql=$(wrapSQL "SELECT cast(1 AS ${a}) AS val UNION SELECT cast(2 AS ${b}) AS val")
    ${CLICKHOUSE_CLIENT} --dialect_type='MYSQL' --enable_implicit_arg_type_convert=1 --query="${sql}"
    sql="SELECT toTypeName(val) from (${sql})"
    ${CLICKHOUSE_CLIENT} --dialect_type='MYSQL' --enable_implicit_arg_type_convert=1 --query="${sql}"
  done
done

# String UNION Non-String
echo "String UNION Non-String"
for x in "CHAR" "INT SIGNED" "INT UNSIGNED" "DECIMAL(3,1)"
do 
  echo $x
  sql=$(wrapSQL "SELECT * FROM (SELECT cast(1 as ${x}) as val UNION SELECT 'hello world' as val) ORDER BY val")
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}"
  sql="SELECT toTypeName(val) from (${sql})"
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}"
done

# Date UNION Non-Date
echo "Date UNION Non-Date"
for x in "CHAR" "BIGINT SIGNED" "BIGINT UNSIGNED" "DECIMAL(3,1)"
do 
  echo $x
  sql=$(wrapSQL "SELECT cast(1 as ${x}) as val UNION SELECT DATE('2023-11-01')")
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}"
  sql="SELECT toTypeName(val) from (${sql})"
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}" 
done

# DateTime UNION Non-DateTime
echo "DateTime UNION Non-DateTime"
for x in "CHAR" "BIGINT SIGNED" "BIGINT UNSIGNED" "DECIMAL(3,1)"
do 
  sql=$(wrapSQL "SELECT cast(1 as ${x}) as val UNION SELECT cast('2023-11-01 12:12:12' as DATETIME)")
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}"
  sql="SELECT toTypeName(val) from (${sql})"
  ${CLICKHOUSE_CLIENT} --dialect_type=MYSQL --enable_implicit_arg_type_convert=1 --query="${sql}"
done
