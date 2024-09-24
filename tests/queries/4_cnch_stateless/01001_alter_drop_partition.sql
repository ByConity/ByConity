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

-- where clause with `in` functions
SELECT '------ DROP PARTITION WHERE ------';
CREATE TABLE t_alter_partition(p Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY (k, p) ORDER BY m;
INSERT INTO t_alter_partition VALUES ('2024-01-01', 1, 1), ('2024-02-01', 1, 2), ('2024-03-01', 1, 3), ('2024-04-01', 2, 4), ('2024-05-01', 2, 5);

SELECT '------ DROP PARTITION (1, 20240101) ------';
ALTER TABLE t_alter_partition DROP PARTITION WHERE p IN ('2024-01-01') AND k = 1;
SELECT * FROM t_alter_partition order by m;

SELECT '------ DROP PARTITION (1, 20240201) ------';
ALTER TABLE t_alter_partition DROP PARTITION WHERE p = '2024-02-01' AND k in (1);
SELECT * FROM t_alter_partition order by m;

SELECT '------ DROP PARTITION (2, 20240401) ------';
ALTER TABLE t_alter_partition DROP PARTITION WHERE p in ('2024-04-01') AND k in (2);
SELECT * FROM t_alter_partition order by m;

SELECT '------ DROP PARTITION (1, 20240301) ------';
ALTER TABLE t_alter_partition DROP PARTITION WHERE _partition_id = '1-20240301';
SELECT * FROM t_alter_partition order by m;

SELECT '------ DROP NON-EIXSTS PARTITION ------';
ALTER TABLE t_alter_partition DROP PARTITION WHERE _partition_value = (1, '20240301');
SELECT * FROM t_alter_partition order by m;

SELECT '------ TRUNCATE PARTITION ------';
CREATE TABLE t_truncate(k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY (k) ORDER BY m;
INSERT INTO t_truncate SELECT number, number FROM numbers(10);

TRUNCATE TABLE t_truncate PARTITION '0';
SELECT count() FROM t_truncate; -- 9

TRUNCATE TABLE t_truncate PARTITION '1';
SELECT count() FROM t_truncate; -- 8

TRUNCATE TABLE t_truncate PARTITION WHERE k = 2;
SELECT count() FROM t_truncate; -- 7

TRUNCATE TABLE t_truncate PARTITION WHERE _partition_id IN ('3', '4');
SELECT count() FROM t_truncate; -- 5

TRUNCATE TABLE t_truncate PARTITION WHERE _partition_value IN (tuple('5'), tuple('6'));
SELECT count() FROM t_truncate; -- 3

TRUNCATE TABLE t_truncate PARTITION '7', '8';
SELECT m FROM t_truncate ORDER BY m; -- 9

TRUNCATE TABLE t_truncate;
SELECT count() FROM t_truncate; -- 0

DROP TABLE t_truncate;

SELECT '------ TRUNCATE CHECK PARTITION ------';
CREATE TABLE t_truncate_nullable_partition(p Nullable(DateTime), k Int32, m Int32)
ENGINE = CnchMergeTree() PARTITION BY (toDate(p), k) ORDER BY m
SETTINGS allow_nullable_key = 1, extract_partition_nullable_date = 1;

INSERT INTO t_truncate_nullable_partition VALUES ('2024-01-01 10:00:00', 1, 1), (NULL, 1, 1);
SELECT count() FROM t_truncate_nullable_partition; -- 2
SELECT _partition_id FROM t_truncate_nullable_partition ORDER BY _partition_id;
TRUNCATE TABLE t_truncate_nullable_partition PARTITION '20240101-1';
SELECT count() FROM t_truncate_nullable_partition; -- 1
