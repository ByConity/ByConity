DROP DATABASE IF EXISTS test_rename_from;
DROP DATABASE IF EXISTS test_rename_to;
CREATE DATABASE test_rename_from;
CREATE TABLE test_rename_from.test (d Date, a String, b String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192;
SYSTEM START MERGES test_rename_from.test;
SYSTEM START GC test_rename_from.test;

INSERT INTO test_rename_from.test VALUES ('2021-01-01', 'hello', 'world');
SELECT * FROM test_rename_from.test;
SELECT 'Background jobs for test_rename_from.test';
--- search for database with like because of tenant id will prefix database name
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database like '%test_rename_from' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database like '%test_rename_from' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
RENAME DATABASE test_rename_from TO test_rename_to;
SELECT * FROM test_rename_to.test;
--- make sure there is enough time for dm and server to exchange message
SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT 'Background jobs for test_rename_from.test after rename';
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database like '%test_rename_from' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database like '%test_rename_from' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT 'Background jobs for test_rename_to.test';
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database like '%test_rename_to' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database like '%test_rename_to' AND table = 'test' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;

DROP DATABASE IF EXISTS test_rename_from;
DROP DATABASE IF EXISTS test_rename_to;
