#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.attach_bug;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.attach_bug2;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.attach_bug (p_date String, id UInt64) ENGINE=CnchMergeTree() ORDER BY id PARTITION BY p_date;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.attach_bug2 AS test.attach_bug;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.attach_bug VALUES ('1',42);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.attach_bug DETACH PARTITION '1';"

# run attach query and force bypass txn cleaner on server
${CLICKHOUSE_CLIENT} --query "ALTER TABLE test.attach_bug2 ATTACH DETACHED PARTITION '1' FROM test.attach_bug SETTINGS force_clean_transaction_by_dm=1;"

# kv commit time is not set because txn cleaner not running
echo `${CLICKHOUSE_CLIENT} --query "SELECT (kv_commit_time > 0) FROM system.cnch_parts WHERE (database = 'test') AND (table = 'attach_bug2');"`

# get the txn id
TXN_ID=$(${CLICKHOUSE_CLIENT} --query "SELECT txn_id FROM system.cnch_transactions WHERE main_table_uuid = (SELECT toString(uuid) FROM system.cnch_tables WHERE database = 'test' AND name = 'attach_bug2') SETTINGS enable_optimizer=0;")

# trigger txn cleaner from dm
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAN TRANSACTION ${TXN_ID}" & sleep 4s

# kv commit time should be set
echo `${CLICKHOUSE_CLIENT} --query "SELECT (kv_commit_time > 0) FROM system.cnch_parts WHERE (database = 'test') AND (table = 'attach_bug2')"`

echo `${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.attach_bug2 ORDER BY p_date"`