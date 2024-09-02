DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
CREATE table t (d Date, t UInt32) Engine = CnchMergeTree ORDER BY t PARTITION BY d;
CREATE table t2 (d Date, t UInt32) Engine = CnchMergeTree ORDER BY t PARTITION BY d;
SYSTEM START MERGES t;
SYSTEM STOP MERGES t2;
SYSTEM START GC t;
SYSTEM STOP GC t2;
SELECT sleepEachRow(3) FROM numbers(2) Format Null;
SYSTEM SUSPEND_ALL MERGES;
SYSTEM SUSPEND_ALL GC;
SELECT sleepEachRow(3) FROM numbers(6) Format Null;
SELECT count() FROM system.dm_bg_jobs WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM system.dm_bg_jobs WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't2';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't2';

SELECT count() FROM system.dm_bg_jobs WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM system.dm_bg_jobs WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't2';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't2';

SYSTEM RESUME_ALL MERGES;
SYSTEM RESUME_ALL GC;

SELECT sleepEachRow(3) FROM numbers(6) Format Null;

SELECT count() FROM system.dm_bg_jobs WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM system.dm_bg_jobs WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't2';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'MergeMutateThread' and database = currentDatabase(0) and table = 't2';

SELECT count() FROM system.dm_bg_jobs WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't';
SELECT count() FROM system.dm_bg_jobs WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't2';
SELECT count() FROM cnch(server, system.bg_threads) WHERE type = 'PartGCThread' and database = currentDatabase(0) and table = 't2';

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
