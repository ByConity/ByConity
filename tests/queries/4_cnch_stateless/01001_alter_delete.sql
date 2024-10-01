set mutations_sync = 1;
DROP TABLE IF EXISTS t_alter_d;
DROP TABLE IF EXISTS t_alter_ids;

CREATE TABLE t_alter_d(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m;
CREATE TABLE t_alter_ids(id Int32) ENGINE = CnchMergeTree ORDER BY id;
-- t_alter_ids: 0,1,2,3,4,5,6,7,8,9
INSERT INTO t_alter_ids select number from numbers(10);
SYSTEM START MERGES t_alter_d;

SELECT '----- DELETE WHERE -----';
-- t_alter_d|1: 5,6,7,8,9,10,11,12,13,14
INSERT INTO t_alter_d select 1, number from numbers(5, 10);

-- t_alter_d|1: 10,11,12,13,14
ALTER TABLE t_alter_d DELETE WHERE m in t_alter_ids;

-- MergeMutateThread will wait ~3 seconds before scheduling.
SELECT count() FROM t_alter_d;

SELECT '----- DELETE IN PARTITION WHERE -----';
-- t_alter_d|2: 6,7,8,9,10,11,12,13,14,15
INSERT INTO t_alter_d select 2, number from numbers(6, 10);

-- t_alter_d|2: 10,11,12,13,14,15
ALTER TABLE t_alter_d DELETE IN PARTITION '2' WHERE m in t_alter_ids;

SELECT count() FROM t_alter_d;

SELECT '----- DELETE NOTHING -----';
-- t_alter_d|1: 10,11,12,13,14; 2: 10,11,12,13,14,15
ALTER TABLE t_alter_d DELETE WHERE m < 10;

SELECT count() FROM t_alter_d;

SELECT '----- DELETE ALL -----';
-- t_alter_d: EMPTY
ALTER TABLE t_alter_d DELETE WHERE 1 = 1;

SELECT count() FROM t_alter_d;

SELECT '----- INSERT AFTER DELETE -----';
-- t_alter_d|1: 1
INSERT INTO t_alter_d VALUES (1,1);

-- t_alter_d|1: EMPTY
ALTER TABLE t_alter_d DELETE WHERE k = 1 and m <= 5;

-- t_alter_d|1: 2
INSERT INTO t_alter_d VALUES (1,2);

SELECT m FROM t_alter_d WHERE k = 1 ORDER BY m;

DROP TABLE t_alter_d;
DROP TABLE t_alter_ids;


SELECT '----- DELETE IN INVALID PARTITION -----';
CREATE TABLE t_alter_d_partition(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY (d, k) ORDER BY m;
SYSTEM START MERGES t_alter_d_partition;
ALTER TABLE t_alter_d_partition DELETE IN PARTITION '20231010-10' WHERE m = 10; -- { serverError 248}

DROP TABLE t_alter_d_partition;

CREATE TABLE t_alter_bad_partition(p DateTime, k Int32) ENGINE = CnchMergeTree PARTITION BY toYYYYMMDD(p) ORDER BY k;
SYSTEM START MERGES t_alter_bad_partition;
ALTER TABLE t_alter_bad_partition ADD INDEX ik(k) TYPE minmax GRANULARITY 1;
ALTER TABLE t_alter_bad_partition MATERIALIZE INDEX ik IN PARTITION '2024-01-01'; -- { serverError 72 }
DROP TABLE t_alter_bad_partition;

CREATE TABLE wrong_column_row_exists(k Int32, _row_exists Int32) ENGINE = CnchMergeTree ORDER BY k; -- { serverError 44 }

SELECT '----- TRIVIAL COUNT AFTER DELETING DATA -----';
CREATE TABLE t_delete_and_trivial_count(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY k;
SYSTEM START MERGES t_delete_and_trivial_count;
INSERT INTO t_delete_and_trivial_count SELECT '2024-01-01', number, number FROM numbers(5);
INSERT INTO t_delete_and_trivial_count SELECT '2024-01-02', number, number FROM numbers(5);
ALTER TABLE t_delete_and_trivial_count DELETE WHERE m < 3;
SELECT count() FROM t_delete_and_trivial_count SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 4
SELECT count() FROM t_delete_and_trivial_count WHERE d = '2024-01-01' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 2
SELECT count() FROM t_delete_and_trivial_count WHERE d = '2024-01-10' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 0
SELECT count() FROM t_delete_and_trivial_count WHERE _partition_id = '20240102' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 2
DROP TABLE t_delete_and_trivial_count;

CREATE TABLE t_delete_and_trivial_count_u(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY k UNIQUE KEY k;
SYSTEM START MERGES t_delete_and_trivial_count_u;
INSERT INTO t_delete_and_trivial_count_u SELECT '2024-01-01', number, number FROM numbers(5);
INSERT INTO t_delete_and_trivial_count_u SELECT '2024-01-02', number, number FROM numbers(5);
ALTER TABLE t_delete_and_trivial_count_u DELETE WHERE m < 3; -- { serverError 36 }
DELETE FROM t_delete_and_trivial_count_u WHERE m < 3;
SELECT count() FROM t_delete_and_trivial_count_u SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 4
SELECT count() FROM t_delete_and_trivial_count_u WHERE d = '2024-01-01' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 2
SELECT count() FROM t_delete_and_trivial_count_u WHERE d = '2024-01-10' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 0
SELECT count() FROM t_delete_and_trivial_count_u WHERE _partition_id = '20240102' SETTINGS enable_optimizer = 1, optimize_trivial_count_query = 1; -- 2
DROP TABLE t_delete_and_trivial_count_u;
