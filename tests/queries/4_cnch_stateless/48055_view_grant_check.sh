#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02184;"
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS view2;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttt;"

$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02184 IDENTIFIED WITH plaintext_password BY 'user_test_02184';"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM user_test_02184;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS ttt (a String, b String) ENGINE = CnchMergeTree ORDER BY (a, b);"
$CLICKHOUSE_CLIENT --query "CREATE VIEW view2 (a String, b String) AS SELECT * FROM ttt WHERE a = 'aaa';"

$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ttt TO user_test_02184 WITH GRANT OPTION;"

[ -v TENANT_ID ] && NEW_USER="${TENANT_ID}\`user_test_02184"
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM ttt settings enable_optimizer=0;" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM ttt settings enable_optimizer=1;" 2>&1| grep -Fo "Not enough privileges" | uniq

echo "---"

$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT a FROM view2 settings enable_optimizer=0;" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT a FROM view2 settings enable_optimizer=1;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT --query "GRANT SELECT ON view2 TO user_test_02184 WITH GRANT OPTION;"

echo "---"

$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT a FROM view2 settings enable_optimizer=0;" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT a FROM view2 settings enable_optimizer=1;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT --query "DROP VIEW view2;"
$CLICKHOUSE_CLIENT --query "DROP TABLE ttt;"
$CLICKHOUSE_CLIENT --query "DROP USER user_test_02184;"
