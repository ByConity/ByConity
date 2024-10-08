CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS t1;
set enable_optimizer=1;
set dialect_type='ANSI';

CREATE TABLE t1(k Int32, a Int32 NULL, b Int32 NOT NULL, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY a;
EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY a ASC NULLS FIRST
    )
        LIMIT 4, 3;


EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY a ASC NULLS LAST
    )
        LIMIT 4, 3;


DROP TABLE IF EXISTS view1;
CREATE VIEW view1 AS SELECT a FROM t1 order by a;

EXPLAIN select * from view1 limit 10;


DROP TABLE IF EXISTS view1;
DROP TABLE IF EXISTS t1;


CREATE TABLE t1(k Int32, b Int32 NOT NULL, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY b;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY b ASC NULLS FIRST
    )
        LIMIT 4, 3;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY b ASC NULLS LAST
    )
        LIMIT 4, 3;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1(k Int32, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY c;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY c ASC NULLS FIRST
    )
        LIMIT 4, 3;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM t1
        ORDER BY c ASC NULLS LAST
    )
        LIMIT 4, 3;

DROP TABLE IF EXISTS t1;