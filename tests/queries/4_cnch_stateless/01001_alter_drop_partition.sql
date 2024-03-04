DROP TABLE IF EXISTS t_alter_partition;

CREATE TABLE t_alter_partition(p DateTime, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY (toDate(p), k) ORDER BY m;

INSERT INTO t_alter_partition SELECT '2024-01-01 11:11:11', number, number from numbers(5);
SYSTEM START MERGES t_alter_partition;

SELECT '------ WRONG PARTITION ID, DROP NOTHING ------';
ALTER TABLE t_alter_partition DROP PARTITION ID '2024-01-01-1';
SELECT count() FROM t_alter_partition WHERE k = 1;

SELECT '------ DROP PARTITION ID 20240101-1 ------';
ALTER TABLE t_alter_partition DROP PARTITION ID '20240101-1';
SELECT count() FROM t_alter_partition WHERE k = 1;

SELECT '------ WRONG PARTITION, DROP NOTHING ------';
ALTER TABLE t_alter_partition DROP PARTITION ('20240101', 6);
SELECT count() FROM t_alter_partition;

SELECT '------ DROP PARTITION (20240101, 2) ------';
ALTER TABLE t_alter_partition DROP PARTITION ('20240101', 2);
SELECT count() FROM t_alter_partition;

ALTER TABLE t_alter_partition DROP PARTITION WHERE m = 5; -- { serverError 47 }

SELECT '------ FINAL STATE ------';
SELECT k FROM t_alter_partition ORDER BY k;

DROP TABLE t_alter_partition;
