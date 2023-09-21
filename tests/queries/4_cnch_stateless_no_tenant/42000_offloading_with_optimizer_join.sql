set enable_optimizer=1;
set offloading_with_query_plan=1;

DROP DATABASE IF EXISTS test_offloading_optimizer;
CREATE DATABASE IF NOT EXISTS test_offloading_optimizer;

DROP TABLE IF EXISTS test_offloading_optimizer.tbl;

CREATE TABLE test_offloading_optimizer.tbl(`id` Int32, i32 Nullable(Int32))
    ENGINE = CnchMergeTree()
    PARTITION BY `id`
    PRIMARY KEY `id`
    ORDER BY `id`
    SETTINGS index_granularity = 8192;

INSERT INTO test_offloading_optimizer.tbl values (1, 0) (2, 1) (3, 2) (4, NULL);

SELECT * FROM test_offloading_optimizer.tbl t1, test_offloading_optimizer.tbl t2 ORDER BY t2.id + t2.i32, t1.id;

SELECT * FROM test_offloading_optimizer.tbl t1, test_offloading_optimizer.tbl t2 ORDER BY t2.id + t2.i32, t1.id limit 5;

SELECT * FROM test_offloading_optimizer.tbl t1 JOIN test_offloading_optimizer.tbl t2 on t1.id = t2.id ORDER BY t1.id;

SELECT COUNT(*) FROM test_offloading_optimizer.tbl t1 JOIN test_offloading_optimizer.tbl t2 on t1.id = t2.id GROUP BY t1.id;

DROP TABLE IF EXISTS test_offloading_optimizer.tbl;

DROP DATABASE IF EXISTS test_offloading_optimizer;