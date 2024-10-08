CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS t;

CREATE TABLE t(a Int32, b Int32, c Int32)
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`
    SETTINGS index_granularity = 8192;

INSERT INTO t VALUES (1, 1, 1), (2, 1, 0), (1, 2, 3), (2, 1, 3), (1, 2, 2), (1, 1, 1);

SELECT a, b, sum(c) FROM t GROUP BY (a, b) ORDER BY (a, b);

DROP TABLE IF EXISTS t;
