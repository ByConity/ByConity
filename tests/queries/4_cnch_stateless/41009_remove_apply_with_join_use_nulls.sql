CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1(a Int32, b Int32, c Int32)
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`;

CREATE TABLE t2(a Int32, b Int32, c Int32)
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`;

CREATE TABLE t3(a Int32, b Int32, c Int32)
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`;


INSERT INTO t1 VALUES (1, 1, 1), (3, 1, 1), (2, 2, 3), (5, 1, 3);
INSERT INTO t2 VALUES (1, 1, 2), (2, 1, 1), (3, 2, 3), (6, 1, 3);
INSERT INTO t3 VALUES (1, 1, 3), (1, 1, 1), (1, 2, 3), (8, 1, 3);

select a from t1 where (a>1 and (a in (select a as a2 from t2))) or (a<10 and (a in (select a as a3 from t3))) order by a SETTINGS join_use_nulls=0, enable_optimizer = 1;

select a from t1 where (a>1 and (a in (select a as a2 from t2))) or (a<10 and (a in (select a as a3 from t3))) order by a SETTINGS join_use_nulls=1, enable_optimizer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

