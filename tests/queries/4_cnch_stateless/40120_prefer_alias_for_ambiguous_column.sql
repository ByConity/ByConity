CREATE DATABASE IF NOT EXISTS test;
USE test;

SET dialect_type = 'MYSQL';

DROP TABLE IF EXISTS t40120_alias_t1;
DROP TABLE IF EXISTS t40120_alias_t2;
DROP TABLE IF EXISTS t40120_alias_t3;

CREATE TABLE t40120_alias_t1
(
    kk Int32 NOT NULL,
    id Int32 NOT NULL
)
ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO t40120_alias_t1 VALUES (1, 1), (2, 1);

CREATE TABLE t40120_alias_t2
(
    kk Int32 NOT NULL,
    app_id Int32 NOT NULL
)
ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO t40120_alias_t2 VALUES (1, 1), (2, 2);

CREATE TABLE t40120_alias_t3
(
    kk Int32 NOT NULL,
    app_id Int32 NOT NULL
)
ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO t40120_alias_t3 VALUES (1, 1), (2, 2), (2, 3);

SELECT 'Q1';

SELECT app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
GROUP BY app_id
ORDER BY app_id, cnt;

SELECT 'Q2';

SELECT app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
JOIN t40120_alias_t3 ON t40120_alias_t1.kk = t40120_alias_t3.kk
GROUP BY app_id
ORDER BY app_id, cnt; -- { serverError 207 }

SELECT 'Q3';

SELECT id AS app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
GROUP BY app_id
ORDER BY app_id, cnt SETTINGS only_full_group_by = 1; -- { serverError 215 }

SELECT id AS app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
GROUP BY app_id
ORDER BY app_id, cnt SETTINGS only_full_group_by = 0;

SELECT 'Q4';

SELECT id AS app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
JOIN t40120_alias_t3 ON t40120_alias_t1.kk = t40120_alias_t3.kk
GROUP BY app_id
ORDER BY app_id, cnt SETTINGS prefer_alias_if_column_name_is_ambiguous = 0; -- { serverError 207 }

SELECT id AS app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
JOIN t40120_alias_t3 ON t40120_alias_t1.kk = t40120_alias_t3.kk
GROUP BY app_id
ORDER BY app_id, cnt SETTINGS prefer_alias_if_column_name_is_ambiguous = 1;
SELECT 'Q5';

SELECT id AS app_id, t40120_alias_t2.app_id as app_id, count(*) as cnt
FROM t40120_alias_t1
JOIN t40120_alias_t2 ON t40120_alias_t1.kk = t40120_alias_t2.kk
JOIN t40120_alias_t3 ON t40120_alias_t1.kk = t40120_alias_t3.kk
GROUP BY app_id
ORDER BY app_id, cnt; -- { serverError 179 }

DROP TABLE IF EXISTS t40120_alias_t1;
DROP TABLE IF EXISTS t40120_alias_t2;
DROP TABLE IF EXISTS t40120_alias_t3;
