#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e
[ -v TENANT_ID ] && NEW_USER="${TENANT_ID}\`my_user"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
DROP USER IF EXISTS my_user;
DROP DATABASE IF EXISTS db;
DROP TABLE IF EXISTS db.tb;

CREATE DATABASE db;
CREATE USER my_user NOT IDENTIFIED;
REVOKE ALL ON *.* FROM my_user;
REVOKE ALL ON db.* FROM my_user;

SET SENSITIVE DATABASE db = 0;
SET SENSITIVE TABLE db.tb = 0;
SET SENSITIVE COLUMN db.tb(id) = 0;
SET SENSITIVE COLUMN db.tb(a) = 0;
SET SENSITIVE COLUMN db.tb(b) = 0;
"""

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SET SENSITIVE DATABASE db = 1;
GRANT DROP DATABASE ON *.* TO my_user;
SELECT '-- drop db';
"""
$CLICKHOUSE_CLIENT --multiline --multiquery --user=$NEW_USER --testmode --query """
DROP DATABASE db; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT DROP DATABASE ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP DATABASE db"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
CREATE DATABASE db;
CREATE TABLE db.tb (id UInt64, a String, b String) ENGINE = CnchMergeTree ORDER BY id;
SET SENSITIVE TABLE db.tb = 1;
GRANT DROP TABLE ON *.* TO my_user;
SELECT '-- drop table';
"""
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
DROP TABLE db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT DROP TABLE ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
DROP TABLE db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT DROP TABLE ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "DROP TABLE db.tb" 

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
CREATE TABLE db.tb (id UInt64, a String, b String) ENGINE = CnchMergeTree ORDER BY id;
REVOKE ALL ON db.* FROM my_user;
SET SENSITIVE TABLE db.tb = 1;
GRANT SELECT ON *.* TO my_user;
SELECT '-- select table';
"""
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT id FROM db.tb; -- { serverError ACCESS_DENIED }
SELECT * FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT id FROM db.tb; -- { serverError ACCESS_DENIED }
SELECT * FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb FORMAT CSV"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb FORMAT CSV"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
REVOKE ALL ON db.* FROM my_user;
SET SENSITIVE COLUMN db.tb(id) = 1;
GRANT SELECT ON *.* TO my_user;
SELECT '-- select column';
"""
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT id FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT id FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT id FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "REVOKE ALL ON db.* FROM my_user;"
$CLICKHOUSE_CLIENT --query "GRANT SELECT(id) ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT id FROM db.tb"
$CLICKHOUSE_CLIENT --user=$NEW_USER --query "SELECT * FROM db.tb"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
REVOKE ALL ON db.* FROM my_user;
SET SENSITIVE COLUMN db.tb(id) = 0;
SET SENSITIVE COLUMN db.tb(a) = 1;
SET SENSITIVE COLUMN db.tb(b) = 1;
GRANT SELECT ON *.* TO my_user;
SELECT '-- select denied';
"""
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT * FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.* TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT * FROM db.tb; -- { serverError ACCESS_DENIED }
"""
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON db.tb TO my_user"
$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SELECT * FROM db.tb; -- { serverError ACCESS_DENIED }
"""

$CLICKHOUSE_CLIENT --user=$NEW_USER --multiline --multiquery --testmode --query """
SET SENSITIVE DATABASE db = 0; -- { serverError ACCESS_DENIED }
SET SENSITIVE TABLE db.tb = 0; -- { serverError ACCESS_DENIED }
SET SENSITIVE COLUMN db.tb(id) = 0; -- { serverError ACCESS_DENIED }
"""

# select from system table
${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SELECT * FROM system.sensitive_grants where user_name like '%my_user' FORMAT CSV;
DROP TABLE IF EXISTS db.tb;
DROP DATABASE IF EXISTS db;
DROP USER my_user;
SET SENSITIVE DATABASE db = 0;
SET SENSITIVE TABLE db.tb = 0;
SET SENSITIVE COLUMN db.tb(id) = 0;
SET SENSITIVE COLUMN db.tb(a) = 0;
SET SENSITIVE COLUMN db.tb(b) = 0;
"""

