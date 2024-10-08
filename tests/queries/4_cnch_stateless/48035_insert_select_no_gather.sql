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
select 'test dedup in write suffix stage';
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
ORDER BY id settings dedup_impl_version = 'dedup_in_write_suffix';

EXPLAIN
INSERT INTO test_unique_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source settings optimize_unique_table_write = 0;

EXPLAIN
INSERT INTO test_unique_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source settings optimize_unique_table_write = 1;

select 'test dedup in txn commit stage';
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
ORDER BY id settings dedup_impl_version = 'dedup_in_txn_commit';

EXPLAIN
INSERT INTO test_unique_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source settings optimize_unique_table_write = 0;

EXPLAIN
INSERT INTO test_unique_insert_bucket SELECT
    p_date,
    id,
    k
FROM test_insert_bucket_source settings optimize_unique_table_write = 1;

DROP TABLE IF EXISTS test_insert_bucket;
DROP TABLE IF EXISTS test_unique_insert_bucket;
DROP TABLE IF EXISTS test_insert_bucket_source;
