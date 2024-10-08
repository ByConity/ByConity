DROP TABLE IF EXISTS test_unique_ignore_mode;
DROP TABLE IF EXISTS test_unique_ignore_mode_version;

SELECT 'test dedup in write suffix stage';
CREATE TABLE test_unique_ignore_mode (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id settings dedup_impl_version = 'dedup_in_write_suffix';;

SET enable_staging_area_for_write = 1, dedup_key_mode = 'ignore';
-- DB::Exception: Only UPSERT mode can write to staging area
INSERT INTO test_unique_ignore_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500); -- { serverError 36 }

SET enable_staging_area_for_write = 0;
-- delete the first row
INSERT INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:40:00', 10001, '10001AA', 11, 1100, 0);
select 'test1', * from test_unique_ignore_mode order by event_time, id;

-- empty block
INSERT INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10001B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10001BB', 22, 2200, 1);
select 'test2', * from test_unique_ignore_mode order by event_time, id;

-- keep the first row
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001C', 3, 300), ('2020-10-29 23:40:00', 10003, '10001CC', 3, 3300);
select 'test3', * from test_unique_ignore_mode order by event_time, id;

INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCC', 333, 33300);
select 'test4', * from test_unique_ignore_mode order by event_time, id;

SET dedup_key_mode = 'replace';
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCCC', 3333, 333300);
select 'test5', * from test_unique_ignore_mode order by event_time, id;

-- test insert throw mode
SET dedup_key_mode = 'throw';
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, 'test throw', 3333, 333300); -- { serverError 117 }
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10004, 'test throw', 3333, 333300), ('2020-10-29 23:40:00', 10004, 'test throw', 3333, 333300); -- { serverError 117 }

DROP TABLE IF EXISTS test_unique_ignore_mode_version;
CREATE TABLE test_unique_ignore_mode_version (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

SET enable_staging_area_for_write = 0, dedup_key_mode = 'ignore';
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:50:00', 10001, '10001AA', 11, 1100, 0), ('2020-10-29 23:55:00', 10001, '10001AA', 111, 11100, 0);
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10002BB', 22, 2200, 0);
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002BBB', 222, 22200, 1);
INSERT INTO test_unique_ignore_mode_version (*) VALUES ('2020-10-29 23:40:00', 10003, '10003C', 3, 300);
select 'test6', * from test_unique_ignore_mode_version order by id, event_time;

TRUNCATE TABLE test_unique_ignore_mode;
TRUNCATE TABLE test_unique_ignore_mode_version;

SELECT '------ INSERT IGNORE INTO ------';

SET dedup_key_mode = 'replace';
SET enable_staging_area_for_write = 1;
INSERT IGNORE INTO test_unique_ignore_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500); -- { serverError 36 }

SET enable_staging_area_for_write = 0;
-- delete the first row
INSERT IGNORE INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:40:00', 10001, '10001AA', 11, 1100, 0);
select 'test1', * from test_unique_ignore_mode order by event_time, id;

-- empty block
INSERT IGNORE INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10001B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10001BB', 22, 2200, 1);
select 'test2', * from test_unique_ignore_mode order by event_time, id;

-- keep the first row
INSERT IGNORE INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001C', 3, 300), ('2020-10-29 23:40:00', 10003, '10001CC', 3, 3300);
select 'test3', * from test_unique_ignore_mode order by event_time, id;

INSERT IGNORE INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCC', 333, 33300);
select 'test4', * from test_unique_ignore_mode order by event_time, id;

INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:50:00', 10001, '10001AA', 11, 1100, 0), ('2020-10-29 23:55:00', 10001, '10001AA', 111, 11100, 0);
INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10002BB', 22, 2200, 0);
INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002BBB', 222, 22200, 1);
INSERT IGNORE INTO test_unique_ignore_mode_version (*) VALUES ('2020-10-29 23:40:00', 10003, '10003C', 3, 300);
select 'test6', * from test_unique_ignore_mode_version order by id, event_time;

DROP TABLE IF EXISTS test_unique_ignore_mode;
DROP TABLE IF EXISTS test_unique_ignore_mode_version;

SELECT '';
SELECT 'test dedup in txn commit stage';
CREATE TABLE test_unique_ignore_mode (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree() PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id settings dedup_impl_version = 'dedup_in_txn_commit';;

SET enable_staging_area_for_write = 1, dedup_key_mode = 'ignore';
-- DB::Exception: Only UPSERT mode can write to staging area
INSERT INTO test_unique_ignore_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500); -- { serverError 36 }

SET enable_staging_area_for_write = 0;
-- delete the first row
INSERT INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:40:00', 10001, '10001AA', 11, 1100, 0);
select 'test1', * from test_unique_ignore_mode order by event_time, id;

-- empty block
INSERT INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10001B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10001BB', 22, 2200, 1);
select 'test2', * from test_unique_ignore_mode order by event_time, id;

-- keep the first row
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001C', 3, 300), ('2020-10-29 23:40:00', 10003, '10001CC', 3, 3300);
select 'test3', * from test_unique_ignore_mode order by event_time, id;

INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCC', 333, 33300);
select 'test4', * from test_unique_ignore_mode order by event_time, id;

SET dedup_key_mode = 'replace';
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCCC', 3333, 333300);
select 'test5', * from test_unique_ignore_mode order by event_time, id;

-- test insert throw mode
SET dedup_key_mode = 'throw';
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, 'test throw', 3333, 333300); -- { serverError 117 }
INSERT INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10004, 'test throw', 3333, 333300), ('2020-10-29 23:40:00', 10004, 'test throw', 3333, 333300); -- { serverError 117 }

DROP TABLE IF EXISTS test_unique_ignore_mode_version;
CREATE TABLE test_unique_ignore_mode_version (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

SET enable_staging_area_for_write = 0, dedup_key_mode = 'ignore';
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:50:00', 10001, '10001AA', 11, 1100, 0), ('2020-10-29 23:55:00', 10001, '10001AA', 111, 11100, 0);
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10002BB', 22, 2200, 0);
INSERT INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002BBB', 222, 22200, 1);
INSERT INTO test_unique_ignore_mode_version (*) VALUES ('2020-10-29 23:40:00', 10003, '10003C', 3, 300);
select 'test6', * from test_unique_ignore_mode_version order by id, event_time;

TRUNCATE TABLE test_unique_ignore_mode;
TRUNCATE TABLE test_unique_ignore_mode_version;

SELECT '------ INSERT IGNORE INTO ------';

SET dedup_key_mode = 'replace';
SET enable_staging_area_for_write = 1;
INSERT IGNORE INTO test_unique_ignore_mode VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500); -- { serverError 36 }

SET enable_staging_area_for_write = 0;
-- delete the first row
INSERT IGNORE INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:40:00', 10001, '10001AA', 11, 1100, 0);
select 'test1', * from test_unique_ignore_mode order by event_time, id;

-- empty block
INSERT IGNORE INTO test_unique_ignore_mode (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10001B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10001BB', 22, 2200, 1);
select 'test2', * from test_unique_ignore_mode order by event_time, id;

-- keep the first row
INSERT IGNORE INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001C', 3, 300), ('2020-10-29 23:40:00', 10003, '10001CC', 3, 3300);
select 'test3', * from test_unique_ignore_mode order by event_time, id;

INSERT IGNORE INTO test_unique_ignore_mode (*) VALUES ('2020-10-29 23:40:00', 10003, '10001CCC', 333, 33300);
select 'test4', * from test_unique_ignore_mode order by event_time, id;

INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10001, '10001A', 1, 100, 1), ('2020-10-29 23:50:00', 10001, '10001AA', 11, 1100, 0), ('2020-10-29 23:55:00', 10001, '10001AA', 111, 11100, 0);
INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002B', 2, 200, 1), ('2020-10-29 23:40:00', 10002, '10002BB', 22, 2200, 0);
INSERT IGNORE INTO test_unique_ignore_mode_version (*, _delete_flag_) VALUES ('2020-10-29 23:40:00', 10002, '10002BBB', 222, 22200, 1);
INSERT IGNORE INTO test_unique_ignore_mode_version (*) VALUES ('2020-10-29 23:40:00', 10003, '10003C', 3, 300);
select 'test6', * from test_unique_ignore_mode_version order by id, event_time;

DROP TABLE IF EXISTS test_unique_ignore_mode;
DROP TABLE IF EXISTS test_unique_ignore_mode_version;
