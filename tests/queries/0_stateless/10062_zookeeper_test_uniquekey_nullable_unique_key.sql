set database_atomic_wait_for_drop_and_detach_synchronously = 1;
CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.test_nullable_unique_key;

CREATE TABLE test_nullable_unique_key(d Date, k Nullable(Int8), v Int32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test_nullable_unique_key', 'rep1')
PARTITION BY d ORDER BY v UNIQUE KEY k SETTINGS partition_level_unique_keys=0; -- { serverError 44 };

DROP TABLE IF EXISTS test.test_nullable_unique_key;
