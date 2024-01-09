SET enable_optimizer=1;
SET optimizer_projection_support=1;
SET force_optimize_projection=1;

CREATE DATABASE IF NOT EXISTS test_proj_selection_db;
USE test_proj_selection_db;

DROP TABLE IF EXISTS test_proj_selection;

CREATE TABLE test_proj_selection
(
    `pkey` Int32,
    `okey` Int32,
    `key1` Int32,
    `val` Int64
)
ENGINE = CnchMergeTree
PARTITION BY pkey
ORDER BY okey;

-- group 1: 5 of 10 parts will be selected

INSERT INTO test_proj_selection(pkey, okey, key1, val)
SELECT number / 100000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000;

-- group 2: 10 of 10 parts will be selected

ALTER TABLE test_proj_selection ADD PROJECTION proj1
    (
SELECT
    pkey,
    key1,
    sum(val)
    GROUP BY pkey, key1
);

INSERT INTO test_proj_selection(pkey, okey, key1, val)
SELECT number / 100000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000 OFFSET 1000000;

-- group 3: 3 of 10 parts will be selected

ALTER TABLE test_proj_selection ADD COLUMN `key2` Int32 AFTER `key1`;

INSERT INTO test_proj_selection(pkey, okey, key1, val)
SELECT number / 100000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000 OFFSET 2000000;

SELECT key1, SUM(val) FROM test_proj_selection WHERE pkey >=5 AND pkey < 23 GROUP BY key1 ORDER BY key1 SETTINGS exchange_timeout_ms = 300000;

DROP TABLE IF EXISTS test_proj_selection;
