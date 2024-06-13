DROP TABLE IF EXISTS t_alter_d;
DROP TABLE IF EXISTS t_alter_ids;

CREATE TABLE t_alter_d(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY k ORDER BY m UNIQUE KEY m SETTINGS enable_delete_mutation_on_unique_table = 1;
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
SELECT sleepEachRow(3) FROM numbers(40) FORMAT Null;
SELECT count() FROM t_alter_d;

SELECT '----- DELETE IN PARTITION WHERE -----';
-- t_alter_d|2: 6,7,8,9,10,11,12,13,14,15
INSERT INTO t_alter_d select 2, number from numbers(6, 10);

-- t_alter_d|2: 10,11,12,13,14,15
ALTER TABLE t_alter_d DELETE IN PARTITION '2' WHERE m in t_alter_ids;

SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT count() FROM t_alter_d;

SELECT '----- DELETE NOTHING -----';
-- t_alter_d|1: 10,11,12,13,14; 2: 10,11,12,13,14,15
ALTER TABLE t_alter_d DELETE WHERE m < 10;

SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT count() FROM t_alter_d;

SELECT '----- DELETE ALL -----';
-- t_alter_d: EMPTY
ALTER TABLE t_alter_d DELETE WHERE 1 = 1;

SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT count() FROM t_alter_d;

SELECT '----- INSERT AFTER DELETE -----';
-- t_alter_d|1: 1
INSERT INTO t_alter_d VALUES (1,1);

-- t_alter_d|1: EMPTY
ALTER TABLE t_alter_d DELETE WHERE k = 1 and m <= 5;

-- t_alter_d|1: 2
INSERT INTO t_alter_d VALUES (1,2);

SELECT sleepEachRow(3) FROM numbers(10) FORMAT Null;
SELECT m FROM t_alter_d WHERE k = 1 ORDER BY m;

SELECT '----- CHECK MUTATION QUERY ID -----';
ALTER TABLE t_alter_d DELETE WHERE m <= 100;
SELECT length(query_id) > 0 FROM system.mutations WHERE database = currentDatabase() and table = 't_alter_d' LIMIT 1;

DROP TABLE t_alter_d;
DROP TABLE t_alter_ids;


SELECT '----- DELETE IN INVALID PARTITION -----';
CREATE TABLE t_alter_d_partition(d Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY (d, k) ORDER BY m;
ALTER TABLE t_alter_d_partition DELETE IN PARTITION '20231010-10' WHERE m = 10; -- { serverError 248}

DROP TABLE t_alter_d_partition;

CREATE TABLE wrong_column_row_exists(k Int32, _row_exists Int32) ENGINE = CnchMergeTree ORDER BY k; -- { serverError 44 }
