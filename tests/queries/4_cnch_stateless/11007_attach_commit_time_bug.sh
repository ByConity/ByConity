#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS attach_bug;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS attach_bug2;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE attach_bug (p_date String, id UInt64) ENGINE=CnchMergeTree() ORDER BY id PARTITION BY p_date;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE attach_bug2 AS attach_bug;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO attach_bug VALUES ('1',42);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE attach_bug DETACH PARTITION '1';"

# run attach query and force bypass txn cleaner on server
${CLICKHOUSE_CLIENT} --query "ALTER TABLE attach_bug2 ATTACH DETACHED PARTITION '1' FROM attach_bug SETTINGS force_clean_transaction_by_dm=1;"

# kv commit time is not set because txn cleaner not running
echo `${CLICKHOUSE_CLIENT} --query "SELECT (kv_commit_time > 0) FROM system.cnch_parts WHERE (database = currentDatabase(1)) AND (table = 'attach_bug2') AND active;"`

# get the txn id
TXN_ID=$(${CLICKHOUSE_CLIENT} --query "SELECT txn_id FROM system.cnch_transactions WHERE main_table_uuid = (SELECT uuid FROM system.cnch_tables WHERE database = currentDatabase(1) AND name = 'attach_bug2') SETTINGS enable_optimizer=0, allow_full_scan_txn_records=1;")

# trigger txn cleaner from dm
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAN TRANSACTION ${TXN_ID}" & sleep 3s

# kv commit time should be set
echo `${CLICKHOUSE_CLIENT} --query "SELECT (kv_commit_time > 0) FROM system.cnch_parts WHERE (database = currentDatabase(1)) AND (table = 'attach_bug2')"`

echo `${CLICKHOUSE_CLIENT} --query "SELECT * FROM attach_bug2 ORDER BY p_date"`
