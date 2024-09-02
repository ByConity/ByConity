CREATE DATABASE IF NOT EXISTS test;
use test;

DROP TABLE IF EXISTS test_insert_bucket;
DROP TABLE IF EXISTS test_insert_bucket_source;

CREATE TABLE test_insert_bucket
(
    `p_date` Date,
    `id` Int32,
    `k` Int32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
CLUSTER BY id INTO 1 BUCKETS
ORDER BY id;

CREATE TABLE test_insert_bucket_source
(
    `p_date` Date,
    `id` Int32,
    `k` Int32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
CLUSTER BY id INTO 1 BUCKETS
ORDER BY id;

set enable_optimizer=1;

EXPLAIN
INSERT INTO test_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source;

select '';
select 'test unique table';
DROP TABLE IF EXISTS test_unique_insert_bucket;

CREATE TABLE test_unique_insert_bucket
(
    `p_date` Date,
    `id` Int32,
    `k` Int32
)
ENGINE = CnchMergeTree
PARTITION BY p_date
UNIQUE KEY id
CLUSTER BY id INTO 1 BUCKETS
ORDER BY id;

EXPLAIN
INSERT INTO test_unique_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source;

DROP TABLE IF EXISTS test_insert_bucket;
DROP TABLE IF EXISTS test_unique_insert_bucket;
DROP TABLE IF EXISTS test_insert_bucket_source;
