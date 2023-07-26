CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.t1;
set enable_optimizer=1;

CREATE TABLE test.t1(a Int32, b Int32) ENGINE = CnchMergeTree() PARTITION BY `a` PRIMARY KEY `a` ORDER BY `a`;
EXPLAIN
SELECT *
FROM
    (
        SELECT *
        FROM test.t1
        ORDER BY a ASC
    )
        LIMIT 4, 3;

DROP TABLE IF EXISTS test.view1;
CREATE VIEW test.view1 AS SELECT a FROM test.t1 order by a;

EXPLAIN select * from test.view1 limit 10;


DROP TABLE IF EXISTS test.view1;
DROP TABLE IF EXISTS test.t1;
