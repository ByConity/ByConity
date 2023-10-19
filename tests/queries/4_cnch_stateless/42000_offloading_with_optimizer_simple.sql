set enable_optimizer=1;
set offloading_with_query_plan=1;

DROP DATABASE IF EXISTS test_offloading_optimizer;
CREATE DATABASE IF NOT EXISTS test_offloading_optimizer;

DROP TABLE IF EXISTS test_offloading_optimizer.t1;

CREATE TABLE test_offloading_optimizer.t1(a Int32, b Int32, c Int32)
    ENGINE = CnchMergeTree()
    PARTITION BY `a`
    PRIMARY KEY `a`
    ORDER BY `a`
    SETTINGS index_granularity = 8192;

INSERT INTO test_offloading_optimizer.t1 VALUES (1, 1, 1), (2, 1, 0), (1, 2, 3), (2, 1, 3), (1, 2, 2), (1, 1, 1);

SELECT * FROM test_offloading_optimizer.t1 ORDER BY (a, b);
SELECT * FROM test_offloading_optimizer.t1 ORDER BY (a, b) LIMIT 2;
SELECT a, sum(c) FROM test_offloading_optimizer.t1 GROUP BY a ORDER BY a;
SELECT * FROM test_offloading_optimizer.t1 where a in (2) and c in (3) ORDER BY a, b;

DROP TABLE IF EXISTS test_offloading_optimizer.t1;

DROP DATABASE IF EXISTS test_offloading_optimizer;
