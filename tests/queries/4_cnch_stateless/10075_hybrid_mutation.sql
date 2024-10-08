DROP TABLE IF EXISTS test_hybrid;
CREATE TABLE test_hybrid (a UInt64, b UInt64) ENGINE = CnchMergeTree() ORDER BY a PARTITION BY (a % 4) SETTINGS index_granularity=128;
INSERT INTO test_hybrid SELECT number, xor(number, 223344) FROM numbers(1024);
INSERT INTO test_hybrid SELECT number, xor(number, 223344) FROM numbers(33);
alter table test_hybrid update b = 0 where true;

SELECT count() FROM test_hybrid;
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) OR (a > 800 AND a < 1000);
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) AND (b > 800 AND b < 1000);

SET enable_hybrid_allocation = 1;
SET min_rows_per_virtual_part = 128;
SELECT count() FROM test_hybrid;
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) OR (a > 800 AND a < 1000);
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) AND (b > 800 AND b < 1000);

SET min_rows_per_virtual_part = 256;
SELECT count() FROM test_hybrid;
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) OR (a > 800 AND a < 1000);
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) AND (b > 800 AND b < 1000);

SET min_rows_per_virtual_part = 512;
SELECT count() FROM test_hybrid;
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) OR (a > 800 AND a < 1000);
SELECT count() FROM test_hybrid WHERE (a > 120 AND a < 300) AND (b > 800 AND b < 1000);
DROP TABLE test_hybrid;
