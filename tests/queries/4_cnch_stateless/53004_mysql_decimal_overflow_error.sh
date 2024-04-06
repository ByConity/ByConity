#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

OPTIONS=" --dialect_type=MYSQL"

function catch_error() 
{
  sql=$1 
  ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "${sql}" 2>&1 | grep -c -m 1 "Code: 69. DB::Exception"
}


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'Integer Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE overflow (x Int NOT NULL, a Decimal( 9, 0) NULL, b Decimal(18, 6) NULL, c Decimal(38, 10)) engine=CnchMergeTree() order by x;"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO overflow VALUES (0, 123456789, 123456789.123456789, -123.345e25);"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO overflow VALUES (7, -123456789, 1.00000001e-20, -123.345e-7);"
catch_error "INSERT INTO overflow (x, a) VALUES (1, 1234567890)"
catch_error "INSERT INTO overflow (x, a) VALUES (2, -1e9)"
catch_error "INSERT INTO overflow (x, b) VALUES (3, 123456789123456789)"
catch_error "INSERT INTO overflow (x, b) VALUES (4, 1234e15)"
catch_error "INSERT INTO overflow (x, c) VALUES (5, -3e28)"
catch_error "INSERT INTO overflow (x, c) VALUES (6, 1.00023e28)"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from overflow order by x"

