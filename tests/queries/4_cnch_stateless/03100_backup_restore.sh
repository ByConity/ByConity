#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUFFIX=$RANDOM

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
-- 03100 backup and restore normal table
DROP TABLE IF EXISTS table_backup_03100;
DROP TABLE IF EXISTS table_restore_03100;

CREATE TABLE table_backup_03100(id UInt32) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO table_backup_03100 SELECT * FROM system.numbers LIMIT 1024;
"""

${CLICKHOUSE_CLIENT} -q "BACKUP TABLE table_backup_03100 TO DISK('hdfs_disk', 'test_backup_03100_${SUFFIX}/') SETTINGS async = 0, id = 'backup_task_03100_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'backup_task_03100_${SUFFIX}';"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE table_backup_03100 AS table_restore_03100 FROM DISK('hdfs_disk', 'test_backup_03100_${SUFFIX}/') SETTINGS async=0, id = 'restore_task_03100_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'restore_task_03100_${SUFFIX}';"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SELECT count() FROM table_backup_03100;
SELECT count() FROM table_restore_03100;

DROP TABLE table_backup_03100;
DROP TABLE table_restore_03100;

-- 03101 backup and restore unique table
DROP TABLE IF EXISTS unique_backup_03101;
DROP TABLE IF EXISTS unique_restore_03101;

CREATE TABLE unique_backup_03101(id UInt32) ENGINE = CnchMergeTree UNIQUE KEY id ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO unique_backup_03101 SELECT * FROM system.numbers LIMIT 1024;

delete from unique_backup_03101 where id > 50;
delete from unique_backup_03101 where id = 1;
"""

${CLICKHOUSE_CLIENT} -q "BACKUP TABLE unique_backup_03101 TO DISK('hdfs_disk', 'test_backup_03101_${SUFFIX}/') SETTINGS async = 0, id = 'backup_task_03101_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'backup_task_03101_${SUFFIX}';"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE unique_backup_03101 AS unique_restore_03101 FROM DISK('hdfs_disk', 'test_backup_03101_${SUFFIX}/') SETTINGS async=0, id = 'restore_task_03101_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'restore_task_03101_${SUFFIX}';"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SELECT count() FROM unique_backup_03101;
SELECT count() FROM unique_restore_03101;

DROP TABLE unique_backup_03101;
DROP TABLE unique_restore_03101;

-- 03102 backup and restore partition
DROP TABLE IF EXISTS partition_backup_03102;
DROP TABLE IF EXISTS partition_restore_03102;

CREATE TABLE partition_backup_03102(id UInt32, group UInt32) ENGINE = CnchMergeTree PARTITION BY group ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO partition_backup_03102 values (1, 1), (2, 1);
INSERT INTO partition_backup_03102 values (1, 2), (2, 2);
"""

${CLICKHOUSE_CLIENT} -q "BACKUP TABLE partition_backup_03102 PARTITIONS '1' TO DISK('hdfs_disk', 'test_backup_03102_${SUFFIX}/') SETTINGS async = 0, id = 'backup_task_03102_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'backup_task_03102_${SUFFIX}';"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE partition_backup_03102 AS partition_restore_03102 PARTITIONS '1' FROM DISK('hdfs_disk', 'test_backup_03102_${SUFFIX}/') SETTINGS async=0, id = 'restore_task_03102_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'restore_task_03102_${SUFFIX}';"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SELECT count() FROM partition_backup_03102;
SELECT count() FROM partition_restore_03102;

DROP TABLE partition_backup_03102;
DROP TABLE partition_restore_03102;

-- 03103 backup and restore whole database
DROP DATABASE IF EXISTS database_backup_03103;
DROP DATABASE IF EXISTS database_restore_03103;

CREATE DATABASE database_backup_03103;
USE database_backup_03103;

CREATE TABLE table_backup(id UInt32) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO table_backup SELECT * FROM system.numbers LIMIT 128;

CREATE TABLE unique_backup(id UInt32) ENGINE = CnchMergeTree UNIQUE KEY id ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO unique_backup SELECT * FROM system.numbers LIMIT 256;
"""

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE database_backup_03103 TO DISK('hdfs_disk', 'test_backup_03103_${SUFFIX}/') SETTINGS async = 0, id = 'backup_task_03103_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'backup_task_03103_${SUFFIX}';"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE database_backup_03103 AS database_restore_03103 FROM DISK('hdfs_disk', 'test_backup_03103_${SUFFIX}/') SETTINGS async=0, id = 'restore_task_03103_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'restore_task_03103_${SUFFIX}';"

${CLICKHOUSE_CLIENT} --multiline --multiquery -q """
SELECT count() FROM database_backup_03103.table_backup;
SELECT count() FROM database_restore_03103.table_backup;
SELECT count() FROM database_backup_03103.unique_backup;
SELECT count() FROM database_restore_03103.unique_backup;

DROP DATABASE database_backup_03103;
DROP DATABASE database_restore_03103;

-- 03104 backup and restore partial database
DROP DATABASE IF EXISTS database_backup_03104;
DROP DATABASE IF EXISTS database_restore_03104;

CREATE DATABASE database_backup_03104;
USE database_backup_03104;

CREATE TABLE table_backup(id UInt32) ENGINE = CnchMergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO table_backup SELECT * FROM system.numbers LIMIT 128;

CREATE TABLE unique_backup(id UInt32) ENGINE = CnchMergeTree UNIQUE KEY id ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO unique_backup SELECT * FROM system.numbers LIMIT 256;
"""

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE database_backup_03104 EXCEPT TABLES unique_backup TO DISK('hdfs_disk', 'test_backup_03104_${SUFFIX}/') SETTINGS async = 0, id = 'backup_task_03104_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'backup_task_03104_${SUFFIX}';"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE database_backup_03104 AS database_restore_03104 FROM DISK('hdfs_disk', 'test_backup_03104_${SUFFIX}/') SETTINGS async=0, id = 'restore_task_03104_${SUFFIX}';" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT status FROM system.cnch_backups where id = 'restore_task_03104_${SUFFIX}';"

${CLICKHOUSE_CLIENT} --multiline --multiquery --testmode -q """
SELECT count() FROM database_backup_03104.table_backup;
SELECT count() FROM database_restore_03104.table_backup;
SELECT count() FROM database_restore_03104.unique_backup; -- { serverError 60 }

DROP DATABASE database_backup_03104;
DROP DATABASE database_restore_03104;
"""
