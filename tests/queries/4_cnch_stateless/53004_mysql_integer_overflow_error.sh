#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

OPTIONS=" --dialect_type=MYSQL"

function catch_error() 
{
  sql=$1 
  ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "${sql}" 2>&1 | grep -c -m 1 "Code: 321. DB::Exception"
}


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'Integer Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE overflow (x Int NOT NULL, a tinyint NULL, b smallint NULL, c int NULL, d bigint NULL) engine=CnchMergeTree() order by x;"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO overflow VALUES (0, 127, 32767, 2147483647, 9223372036854775807);"
catch_error "INSERT INTO overflow (x, a) VALUES (1, -129)"
catch_error "INSERT INTO overflow (x, a) VALUES (2, 128)"
catch_error "INSERT INTO overflow (x, b) VALUES (3, 32768)"
catch_error "INSERT INTO overflow (x, b) VALUES (4, -32769)"
catch_error "INSERT INTO overflow (x, c) VALUES (5, 2147483648)"
catch_error "INSERT INTO overflow (x, c) VALUES (6, -2147483649)"
catch_error "INSERT INTO overflow (x, d) VALUES (7, 9223372036854775808)"
catch_error "INSERT INTO overflow (x, d) VALUES (8, -9223372036854775809)"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from overflow order by x"
