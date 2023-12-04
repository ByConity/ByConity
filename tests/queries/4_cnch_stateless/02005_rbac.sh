#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02184;"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02185;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS int_table;"
$CLICKHOUSE_CLIENT --query "DROP ROLE IF EXISTS role1;"

$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02184 IDENTIFIED WITH plaintext_password BY 'user_test_02184';"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM user_test_02184;"

$CLICKHOUSE_CLIENT --query "GRANT CREATE ON *.* TO user_test_02184;"
[ -v TENANT_ID ] && NEW_USER="${TENANT_ID}\`user_test_02184"
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS int_table (a int) engine = CnchMergeTree ORDER BY a;"
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM int_table;" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "SELECT * FROM int_table settings enable_optimizer=1;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "CREATE TABLE t AS int_table;"
$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE t;"
$CLICKHOUSE_CLIENT  --user=$NEW_USER --password=user_test_02184  --query "DROP TABLE t;" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT --query "CREATE ROLE role1;"
$CLICKHOUSE_CLIENT --query "GRANT DROP ON *.* TO role1;"
$CLICKHOUSE_CLIENT --query "GRANT role1 to user_test_02184;"
$CLICKHOUSE_CLIENT --query "ALTER USER user_test_02184 DEFAULT ROLE role1;"
$CLICKHOUSE_CLIENT --query "SHOW GRANTS FOR user_test_02184 FORMAT CSV;"
$CLICKHOUSE_CLIENT --query "SELECT * from system.role_grants FORMAT CSV;"

# In next login, default should apply to user_test_02184 and he can drop table
$CLICKHOUSE_CLIENT --user=$NEW_USER --password=user_test_02184  --query "DROP TABLE t;"

# Create new user user_test_02185 and user_test_02184 will grant SELECT permissions to user_test_02185
$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02185 IDENTIFIED WITH plaintext_password BY 'user_test_02185';"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON int_table TO user_test_02184 WITH GRANT OPTION;"
$CLICKHOUSE_CLIENT --query "INSERT INTO int_table VALUES (1);"
$CLICKHOUSE_CLIENT --user=$NEW_USER --password=user_test_02184 --query "GRANT SELECT ON int_table TO user_test_02185;"
[ -v TENANT_ID ] && NEW_USER1="${TENANT_ID}\`user_test_02185"
$CLICKHOUSE_CLIENT --user=$NEW_USER1 --password=user_test_02185 --query "SELECT a FROM int_table;"
$CLICKHOUSE_CLIENT --user=$NEW_USER1 --password=user_test_02185 --query "SELECT a FROM int_table settings enable_optimizer=1;"


$CLICKHOUSE_CLIENT --query "DROP TABLE int_table;"
$CLICKHOUSE_CLIENT --query "DROP USER user_test_02184;"
$CLICKHOUSE_CLIENT --query "DROP USER user_test_02185;"
$CLICKHOUSE_CLIENT --query "DROP ROLE role1;"
