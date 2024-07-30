DROP TABLE IF EXISTS rename1;
DROP TABLE IF EXISTS rename2;
DROP TABLE IF EXISTS rename11;
DROP TABLE IF EXISTS rename22;
CREATE TABLE rename1 (d Date, a String, b String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192;
CREATE TABLE rename2 (d Date, a String, b String) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(d) ORDER BY d SETTINGS index_granularity = 8192;
SYSTEM START MERGES rename1;
SYSTEM START GC rename1;
INSERT INTO rename1 VALUES ('2015-01-01', 'hello', 'world');
INSERT INTO rename2 VALUES ('2015-01-02', 'hello2', 'world2');
SELECT * FROM rename1;
SELECT * FROM rename2;
SELECT 'Background jobs for rename1';
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database = currentDatabase(0) AND table = 'rename1' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database = currentDatabase(0) AND table = 'rename1' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
RENAME TABLE rename1 TO rename11, rename2 TO rename22;
SELECT * FROM rename11;
SELECT * FROM rename22;

--- make sure there is enough time for dm and server to exchange message
SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT 'Background jobs for rename1 after rename';
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database = currentDatabase(0) AND table = 'rename1' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database = currentDatabase(0) AND table = 'rename1' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT 'Background jobs for rename11';
SELECT type, table, status, expected_status FROM system.dm_bg_jobs WHERE database = currentDatabase(0) AND table = 'rename11' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;
SELECT type, table, status FROM system.bg_threads WHERE database = currentDatabase(0) AND table = 'rename11' AND type IN ('PartGCThread', 'MergeMutateThread') ORDER BY type;

DROP TABLE IF EXISTS rename1;
DROP TABLE IF EXISTS rename2;
DROP TABLE IF EXISTS rename11;
DROP TABLE IF EXISTS rename22;
