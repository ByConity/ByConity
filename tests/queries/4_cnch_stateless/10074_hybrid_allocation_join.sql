DROP TABLE IF EXISTS test_hybrid_join;
DROP TABLE IF EXISTS test_normal;

CREATE TABLE test_hybrid_join (a UInt64, b UInt64) ENGINE = CnchMergeTree() ORDER BY (a, b) PARTITION BY (a % 4) SETTINGS index_granularity=128, enable_hybrid_allocation = 1, min_rows_per_virtual_part = 128;
INSERT INTO test_hybrid_join SELECT number, xor(number, 223344) FROM numbers(1024);

CREATE TABLE test_normal (a UInt64, b UInt64) ENGINE = CnchMergeTree() ORDER BY (a, b) PARTITION BY (a % 4) SETTINGS index_granularity=128;
INSERT INTO test_normal SELECT number, xor(number, 223344) FROM numbers(1024);

SELECT count() FROM (SELECT * from test_hybrid_join as t1 LEFT JOIN test_normal as t2 ON t1.a = t2.a);
SELECT count() FROM (SELECT * from test_hybrid_join as t1 RIGHT JOIN test_normal as t2 ON t1.a = t2.a) SETTINGS enable_optimizer = 1;
SELECT count() FROM (SELECT * from test_hybrid_join as t1 INNER JOIN test_normal as t2 ON t1.a = t2.a);

DROP TABLE test_hybrid_join;
DROP TABLE test_normal;
