#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS my_user;"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS db.tb;"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE db;"
$CLICKHOUSE_CLIENT --query "CREATE USER my_user NOT IDENTIFIED;"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON *.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"

$CLICKHOUSE_CLIENT --query "SET SENSITIVE DATABASE db = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE TABLE db.tb = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(id) = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(a) = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(b) = 0;"

[ -v TENANT_ID ] && NEW_USER="${TENANT_ID}\`my_user"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE DATABASE db = 1;"
$CLICKHOUSE_CLIENT --query "GRANT DROP DATABASE ON *.* TO my_user"

$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP DATABASE db" 2>&1| grep -Fo "Not enough privileges" | uniq

$CLICKHOUSE_CLIENT --query "GRANT DROP DATABASE ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP DATABASE db"

$CLICKHOUSE_CLIENT --query "CREATE DATABASE db;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE db.tb (id UInt64, a String, b String) ENGINE = CnchMergeTree ORDER BY id;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE TABLE db.tb = 1"
$CLICKHOUSE_CLIENT --query "GRANT DROP TABLE ON *.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP TABLE db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT DROP TABLE ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP TABLE db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT DROP TABLE ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP TABLE db.tb" 

$CLICKHOUSE_CLIENT --query "CREATE TABLE db.tb (id UInt64, a String, b String) ENGINE = CnchMergeTree ORDER BY id;"
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE TABLE db.tb = 1"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON *.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb FORMAT CSV"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb FORMAT CSV"

$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(id) = 1"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON *.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(id) ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb"

$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(id) = 0"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(a) = 1"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(b) = 1"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON *.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb" 2>&1| grep -Fo "Not enough privileges" | uniq

# select from system table
$CLICKHOUSE_CLIENT --query "SELECT * FROM system.sensitive_grants where user_name like '%my_user' FORMAT CSV"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS db.tb;"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db;"
$CLICKHOUSE_CLIENT --query "DROP USER my_user;"

$CLICKHOUSE_CLIENT --query "SET SENSITIVE DATABASE db = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE TABLE db.tb = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(id) = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(a) = 0;"
$CLICKHOUSE_CLIENT --query "SET SENSITIVE COLUMN db.tb(b) = 0;"
