CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.t1;
set enable_optimizer=1;
set dialect_type='ANSI';

CREATE TABLE test.t1(k Int32, a Int32 NULL, b Int32 NOT NULL, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY a;
EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY a ASC NULLS FIRST
    )
        LIMIT 4, 3;


EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY a ASC NULLS LAST
    )
        LIMIT 4, 3;


DROP TABLE IF EXISTS test.view1;
CREATE VIEW test.view1 AS SELECT a FROM test.t1 order by a;

EXPLAIN select * from test.view1 limit 10;


DROP TABLE IF EXISTS test.view1;
DROP TABLE IF EXISTS test.t1;


CREATE TABLE test.t1(k Int32, b Int32 NOT NULL, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY b;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY b ASC NULLS FIRST
    )
        LIMIT 4, 3;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY b ASC NULLS LAST
    )
        LIMIT 4, 3;

DROP TABLE IF EXISTS test.t1;

CREATE TABLE test.t1(k Int32, c Float64 NOT NULL) ENGINE = CnchMergeTree() PARTITION BY `k` ORDER BY c;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY c ASC NULLS FIRST
    )
        LIMIT 4, 3;

EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY c ASC NULLS LAST
    )
        LIMIT 4, 3;

DROP TABLE IF EXISTS test.t1;