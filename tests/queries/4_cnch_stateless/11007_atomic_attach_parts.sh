#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS atomic_attach_parts"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE atomic_attach_parts (p_date String, id UInt64) ENGINE=CnchMergeTree() ORDER BY id PARTITION BY p_date;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO atomic_attach_parts VALUES ('1',42);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE atomic_attach_parts ATTACH PARTS FROM '${HDFS_PATH_ROOT}/some_never_used_directory/' SETTINGS force_clean_transaction_by_dm = 1, cnch_atomic_attach_part = 1, cnch_atomic_attach_part_preemtive_lock_acquire = 1;"

DIR_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT directory FROM system.cnch_fs_lock WHERE database = currentDatabase(0) AND table = 'atomic_attach_parts';")

if [ "${DIR_PATH}" == "${HDFS_PATH_ROOT}/some_never_used_directory" ]; then
    echo "directory is correct"
else
    echo "PATH is not correct, please check SELECT directory FROM system.cnch_fs_lock WHERE database = currentDatabase(0) AND table = 'atomic_attach_parts'; the expected result is ${HDFS_PATH_ROOT}/some_never_used_directory"
    exit -1
fi

echo `${CLICKHOUSE_CLIENT} --query "ALTER TABLE atomic_attach_parts ATTACH PARTS FROM '${HDFS_PATH_ROOT}/some_never_used_directory/' SETTINGS cnch_atomic_attach_part = 1;" 2>&1 | grep -c -m 1 "Cannot lock directory"`

echo `${CLICKHOUSE_CLIENT} --query "ALTER TABLE atomic_attach_parts ATTACH PARTS FROM '${HDFS_PATH_ROOT}/some_never_used_directory/' SETTINGS cnch_atomic_attach_part = 1;" 2>&1 | grep -c -m 1 "Cannot lock directory"`

echo `${CLICKHOUSE_CLIENT} --query "ALTER TABLE atomic_attach_parts ATTACH PARTS FROM '${HDFS_PATH_ROOT}/some_never_used_directory/inner_directory/' SETTINGS cnch_atomic_attach_part = 1;" 2>&1 | grep -c -m 1 "Cannot lock directory"`

TXN_ID=$(${CLICKHOUSE_CLIENT} --query "SELECT txn_id FROM system.cnch_fs_lock WHERE database = currentDatabase(0) AND table = 'atomic_attach_parts';")

if [ "${TXN_ID}" == "" ]; then
    echo "No transaction id in lock table, terminate on error" >& 2
    exit -1
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAN TRANSACTION ${TXN_ID}" & sleep 2s

${CLICKHOUSE_CLIENT} --query "SELECT directory FROM system.cnch_fs_lock WHERE database = currentDatabase(0) AND table = 'atomic_attach_parts';"

echo `${CLICKHOUSE_CLIENT} --query "SELECT * FROM atomic_attach_parts;"`
