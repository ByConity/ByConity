#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

## Masking policy
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS col_masking;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY IF EXISTS credit_card_mask_test;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY IF EXISTS credit_card_mask_test1;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE col_masking (name String, age Int64, credit_card_num String) ENGINE = CnchMergeTree() PARTITION BY name PRIMARY KEY name ORDER BY name;"

# Masking policy already exists
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY credit_card_mask_test AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE val END;"
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY credit_card_mask_test AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE val END;" 2>&1 | grep -o -m 1 "Code: 490"
# ${CLICKHOUSE_CURL} -sS ${CLICKHOUSE_URL} -d "CREATE MASKING POLICY credit_card_mask_test AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE val END;" 2>&1 | grep -o "Code: 490"

# Illegal type Int64 of argument of function substring.
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY credit_card_mask_test1 AS (val Int64) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE val END;" 2>&1 | grep -o -m 1 "Code: 43"

# There is no supertype for types String, UInt8 because some of them are String/FixedString and some of them are not.
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY credit_card_mask_test1 AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('**', substr(val, 1, 3), '**') ELSE 123 END;" 2>&1 | grep -o -m 1 "Code: 386"

# Case expression return type and input column type mismatch
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY credit_card_mask_test1 AS (val String) -> CASE WHEN 123 IN (123, 345) THEN 456 ELSE 123 END;" 2>&1 | grep -o -m 1 "Code: 493"

# Alter masking policy that does not exist
$CLICKHOUSE_CLIENT --query="ALTER MASKING POLICY credit_card_mask_test123 AS (val String) -> CASE WHEN 123 IN (123, 345) THEN concat('$$', substr(val, 1, 3), '$$') ELSE val END;" 2>&1 | grep -o -m 1 "Code: 492"

# Drop table without dropping masking policy applied on it
$CLICKHOUSE_CLIENT --query="ALTER TABLE col_masking MODIFY COLUMN credit_card_num SET MASKING POLICY credit_card_mask_test;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY credit_card_mask_test;" 2>&1 | grep -o -m 1 "Code: 494"

# Drop a masking policy that doesn't exist
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY credit_card_mask_test123;" 2>&1 | grep -o -m 1 "Code: 492"

# Masking policy does not support PREWHERE
$CLICKHOUSE_CLIENT --query="SELECT name FROM col_masking PREWHERE age = 12" 2>&1 | grep -o -m 1 "Code: 182"

# Mixing of commands in ALTER TABLE
# $CLICKHOUSE_CLIENT --query="ALTER TABLE col_masking MODIFY COLUMN credit_card_num UNSET MASKING POLICY, COMMENT COLUMN credit_card_num 'comment';" 2>&1 | grep -o -m 1 "Code: 495"

$CLICKHOUSE_CLIENT --query="ALTER TABLE col_masking MODIFY COLUMN credit_card_num UNSET MASKING POLICY;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY credit_card_mask_test;"
$CLICKHOUSE_CLIENT --query="DROP TABLE col_masking;"

## Row policy
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS row_policy;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS row_policy_mismatch;"
$CLICKHOUSE_CLIENT --query="DROP ROW ACCESS POLICY IF EXISTS rp;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY IF EXISTS int_mask;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS rp_mapping_table;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE rp_mapping_table (x UInt64) Engine = CnchMergeTree ORDER BY x;"
$CLICKHOUSE_CLIENT --query="CREATE TABLE row_policy (v UInt64, w UInt64) Engine = CnchMergeTree ORDER BY v;"
$CLICKHOUSE_CLIENT --query="CREATE ROW ACCESS POLICY rp AS (x UInt64, y UInt64) -> CASE WHEN current_role() = 1 THEN x < 3 WHEN current_role() = 2 THEN y < 3 ELSE 1 END;"
$CLICKHOUSE_CLIENT --query="CREATE MASKING POLICY int_mask AS (val UInt64) -> CASE WHEN 123 IN (123, 345) THEN 0 ELSE val END;"

# Cannot create table with row policy with mismatch in columns
$CLICKHOUSE_CLIENT --query="CREATE TABLE row_policy_mismatch (v String, w UInt64) Engine = CnchMergeTree ORDER BY v ROW ACCESS POLICY rp on (v,w);" 2>&1 | grep -o -m 1 "Code: 53"

# Cannot create row policy with ambiguous input columns
$CLICKHOUSE_CLIENT --query="CREATE ROW ACCESS POLICY ambiguous_rp AS (x UInt64, y UInt64) -> CASE WHEN exists((SELECT x from rp_mapping_table WHERE x = x)) THEN 1 ELSE 0 END;" 2>&1 | grep -o -m 1 "Code: 49"

# Masking policy and Row policy cannot be applied to the same column at the same time
$CLICKHOUSE_CLIENT --query="ALTER TABLE row_policy SET ROW ACCESS POLICY rp ON (v, w);"
$CLICKHOUSE_CLIENT --query="ALTER TABLE row_policy MODIFY COLUMN v SET MASKING POLICY int_mask;" 2>&1 | grep -o -m 1 "Code: 6001"

# Row policy does not support PREWHERE
$CLICKHOUSE_CLIENT --query="SELECT v FROM row_policy PREWHERE v = 1" 2>&1 | grep -o -m 1 "Code: 182"

$CLICKHOUSE_CLIENT --query="ALTER TABLE row_policy UNSET ROW ACCESS POLICY;"
$CLICKHOUSE_CLIENT --query="DROP TABLE row_policy;"
$CLICKHOUSE_CLIENT --query="DROP TABLE rp_mapping_table;"
$CLICKHOUSE_CLIENT --query="DROP MASKING POLICY int_mask;"
$CLICKHOUSE_CLIENT --query="DROP ROW ACCESS POLICY rp;"
