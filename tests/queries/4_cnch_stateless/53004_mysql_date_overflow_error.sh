#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

OPTIONS=" --dialect_type=MYSQL"

function catch_error() 
{
  sql=$1 
  ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "${sql}" 2>&1 | grep -c -m 1 "Code: 321. DB::Exception"
}


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'Date Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS date_overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE date_overflow (x Int NOT NULL, d Date NOT NULL) engine=CnchMergeTree() order by x"
catch_error "INSERT INTO date_overflow VALUES (1, '1969-12-31')"
catch_error "INSERT INTO date_overflow VALUES (4, '2149-06-07')" 
catch_error "INSERT INTO date_overflow FORMAT CSV 5,'1900-12-31'" 
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2149-07-07"}'
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow  {"x": 7, "d": "2024-13-01"}'
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow  {"x": 8, "d": "2024-11-00"}'
echo "2	1970-01-01" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated" 
echo "3	2149-06-06" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated" 
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from date_overflow order by x"


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'Date32 Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS date_overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE date_overflow (x Int NOT NULL, d Date32 NOT NULL) engine=CnchMergeTree() order by x"
catch_error "INSERT INTO date_overflow VALUES (1, '1899-12-31')"
catch_error "INSERT INTO date_overflow VALUES (4, '2499-12-31')"
catch_error "INSERT INTO date_overflow FORMAT CSV 5,'1000-12-31'"
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-01"}'
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 7, "d": "2024-13-01"}'
echo "2	1900-01-01" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated" 
echo "3	2299-12-31" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated" 
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from date_overflow order by x"


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'DateTime Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS date_overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE date_overflow (x Int NOT NULL, d DateTime('UTC') NOT NULL) engine=CnchMergeTree() order by x"
catch_error "INSERT INTO date_overflow VALUES (8, '0000-01-01 00:00:00')"
catch_error "INSERT INTO date_overflow VALUES (0, '0000-01-01 23:59:59')"
catch_error "INSERT INTO date_overflow VALUES (1, '1900-01-01 23:59:59')"
catch_error "INSERT INTO date_overflow VALUES (4, '2106-02-08 00:00:00')"
catch_error "INSERT INTO date_overflow FORMAT CSV 5,'1969-12-31 23:59:59'"
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2106-02-07 06:28:16"}'
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 7, "d": "2024-13-01 11:12:12"}'
echo "2	1970-01-01 00:00:00" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated"
echo "3	2106-02-07 06:28:15" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query  "INSERT INTO date_overflow FORMAT TabSeparated"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from date_overflow order by x"


${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT 'DateTime64 Throw'"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "DROP TABLE IF EXISTS date_overflow"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "CREATE TABLE date_overflow (x Int NOT NULL, d DateTime64(3, 'UTC') NOT NULL) engine=CnchMergeTree() order by x"
catch_error "INSERT INTO date_overflow VALUES (1, '1000-01-01 23:59:59.123')"
catch_error "INSERT INTO date_overflow VALUES (4, '2299-12-32 00:00:00')"
catch_error "INSERT INTO date_overflow FORMAT CSV 5,'1899-12-31 23:59:59.999'"
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 6, "d": "2300-01-01 00:00:00"}'
catch_error 'INSERT INTO date_overflow FORMAT JSONEachRow {"x": 7, "d": "2024-13-01 11:12:12.123"}'
echo "2	1900-01-01 00:00:00" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated"
echo "3	2299-12-31 23:59:59.9999" | ${CLICKHOUSE_CLIENT} ${OPTIONS} --query "INSERT INTO date_overflow FORMAT TabSeparated"
${CLICKHOUSE_CLIENT} ${OPTIONS} --query "SELECT * from date_overflow order by x"
