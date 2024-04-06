#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

OPTIONS=" --dialect_type=MYSQL"

function catch_error() 
{
  sql=$1 
  ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "${sql}" 2>&1 | grep -c -m 1 "Code: 321"
}


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'Time Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE overflow (x Int NOT NULL, a Time NULL) engine=CnchMergeTree() order by x;"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO overflow VALUES (0, '00:00:00')";
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO overflow VALUES (7, '23:59:59')";
catch_error "INSERT INTO overflow (x, a) VALUES (1, '24:00:00')"
catch_error "INSERT INTO overflow (x, a) VALUES (2, '23:60:00')"
catch_error "INSERT INTO overflow (x, a) VALUES (3, '01:59:60')"
catch_error "INSERT INTO overflow (x, a) VALUES (4, '11:11:61')"
catch_error "INSERT INTO overflow (x, a) VALUES (5, '1:80:12')"
catch_error "INSERT INTO overflow (x, a) VALUES (6, '1:12:80')"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from overflow order by x"
