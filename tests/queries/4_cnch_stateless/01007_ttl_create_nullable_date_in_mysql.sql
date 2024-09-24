DROP TABLE IF EXISTS t_ck_ttl_nullable_date;

-- can't use nullable date ttl column by default
CREATE TABLE t_ck_ttl_nullable_date_by_default(p Nullable(Date), k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY p ORDER BY k TTL p + INTERVAL 1 DAY; -- { serverError 450}

-- can use nullable date ttl column if allow_nullable_key is enabled.
CREATE TABLE t_ck_ttl_nullable_date(p Nullable(Date), k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY p ORDER BY k TTL p + INTERVAL 1 DAY SETTINGS allow_nullable_key = 1;

DROP TABLE t_ck_ttl_nullable_date;


SET dialect_type = 'MYSQL';

DROP TABLE IF EXISTS t_mysql_ttl_nullable_date;

-- can use nullable date ttl column in mysql dialect.

SELECT '------ TEST CREATE QUERY ------';
CREATE TABLE t_mysql_ttl_nullable_date(p Date, k Int32, m Int32) ENGINE = CnchMergeTree PARTITION BY p ORDER BY k TTL p + INTERVAL 1 DAY;
SYSTEM START MERGES t_mysql_ttl_nullable_date;
SYSTEM STOP MERGES t_mysql_ttl_nullable_date;


SELECT '------ TEST INSERT QUERY ------';
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 1, 1);

-- switch to CLICKHOUSE dialect
SET dialect_type = 'CLICKHOUSE';
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 2, 2);
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 3, 3);
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 4, 4);
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 5, 5);
INSERT INTO t_mysql_ttl_nullable_date VALUES (today(), 6, 6);

SELECT '------ TEST TTL ------';
INSERT INTO t_mysql_ttl_nullable_date VALUES (today() - 1, 1, 1);

SELECT '------ TEST MERGE ------';
SET mutations_sync = 1;
OPTIMIZE TABLE t_mysql_ttl_nullable_date;

SELECT '------ TEST SELECT ------';
SELECT count() FROM t_mysql_ttl_nullable_date;

-- need to start MergeMutateThread before waiting for mutations
SYSTEM START MERGES t_mysql_ttl_nullable_date;

SELECT '------ TEST ALTER ------';
ALTER TABLE t_mysql_ttl_nullable_date MODIFY SETTING cnch_merge_enable_batch_select = 1;

ALTER TABLE t_mysql_ttl_nullable_date MODIFY COLUMN m Nullable(UInt64);
SELECT count() FROM t_mysql_ttl_nullable_date;


DROP TABLE t_mysql_ttl_nullable_date;
