
DROP TABLE IF EXISTS t_alter_conflict_test;

CREATE TABLE t_alter_conflict_test(`user_id` UInt32,`age` UInt32,`d` UInt32,`str` String) ENGINE = CnchMergeTree PARTITION BY user_id ORDER BY user_id;

INSERT INTO t_alter_conflict_test SELECT number, 1, 2, '1' FROM system.numbers limit 10;

SYSTEM START MERGES t_alter_conflict_test;

SELECT 'TEST MODIFY COLUMN WITH CONFLICT';

ALTER TABLE t_alter_conflict_test MODIFY COLUMN str Int32;

ALTER TABLE t_alter_conflict_test MODIFY COLUMN str String; -- { serverError 517}

ALTER TABLE t_alter_conflict_test RENAME COLUMN str TO str1; -- { serverError 517}

-- wait task finish
SELECT sleepEachRow(3) FROM numbers(5) FORMAT Null;

SELECT str FROM t_alter_conflict_test SETTINGS enable_optimizer = 0;
SELECT str FROM t_alter_conflict_test SETTINGS enable_optimizer = 1;

DROP TABLE IF EXISTS t_alter_conflict_test;

SELECT 'TEST RENAME COLUMN WITH CONFLICT';

DROP TABLE IF EXISTS t_alter_rename_conflict_test;

CREATE TABLE t_alter_rename_conflict_test(`user_id` UInt32,`age` UInt32,`d` UInt32,`str` String) ENGINE = CnchMergeTree PARTITION BY user_id ORDER BY user_id;

INSERT INTO t_alter_rename_conflict_test SELECT number, 1, 2, '1' FROM system.numbers limit 10;

-- wait task finish
SELECT sleepEachRow(3) FROM numbers(5) FORMAT Null;

ALTER TABLE t_alter_rename_conflict_test RENAME COLUMN str TO str1;

ALTER TABLE t_alter_rename_conflict_test RENAME COLUMN str1 TO str2; -- { serverError 517}

ALTER TABLE t_alter_rename_conflict_test RENAME COLUMN str TO str2; -- { serverError 10}

SELECT str1 FROM t_alter_rename_conflict_test SETTINGS enable_optimizer = 0;
SELECT str1 FROM t_alter_rename_conflict_test SETTINGS enable_optimizer = 1;

DROP TABLE IF EXISTS t_alter_rename_conflict_test;
