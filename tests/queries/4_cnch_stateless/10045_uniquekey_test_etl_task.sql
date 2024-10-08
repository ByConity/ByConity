DROP TABLE IF EXISTS test_unique_etl_task;

SELECT 'test dedup in write suffix stage';
CREATE TABLE test_unique_etl_task (d Date, k UInt32, m UInt64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k UNIQUE KEY k settings dedup_impl_version = 'dedup_in_write_suffix';

SET optimize_unique_table_write = 0;
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(0);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(1000);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number * 10 FROM system.numbers limit 500, 500;
INSERT INTO test_unique_etl_task SELECT d, k, m - 1 FROM test_unique_etl_task limit 200, 200;
SELECT count(), sum(k), sum(m) FROM test_unique_etl_task; 

TRUNCATE TABLE test_unique_etl_task;
SET optimize_unique_table_write = 1;
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(0);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(1000);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number * 10 FROM system.numbers limit 500, 500;
INSERT INTO test_unique_etl_task SELECT d, k, m - 1 FROM test_unique_etl_task limit 200, 200;
SELECT count(), sum(k), sum(m) FROM test_unique_etl_task; 

DROP TABLE IF EXISTS test_unique_etl_task;

SELECT '';
SELECT 'test dedup in txn commit stage';
CREATE TABLE test_unique_etl_task (d Date, k UInt32, m UInt64) ENGINE = CnchMergeTree() PARTITION BY d ORDER BY k UNIQUE KEY k settings dedup_impl_version = 'dedup_in_txn_commit';

SET optimize_unique_table_write = 0;
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(0);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(1000);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number * 10 FROM system.numbers limit 500, 500;
INSERT INTO test_unique_etl_task SELECT d, k, m - 1 FROM test_unique_etl_task limit 200, 200;
SELECT count(), sum(k), sum(m) FROM test_unique_etl_task; 

TRUNCATE TABLE test_unique_etl_task;
SET optimize_unique_table_write = 1;
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(0);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number FROM numbers(1000);
INSERT INTO test_unique_etl_task SELECT '2024-08-13', number, number * 10 FROM system.numbers limit 500, 500;
INSERT INTO test_unique_etl_task SELECT d, k, m - 1 FROM test_unique_etl_task limit 200, 200;
SELECT count(), sum(k), sum(m) FROM test_unique_etl_task; 

DROP TABLE IF EXISTS test_unique_etl_task;
