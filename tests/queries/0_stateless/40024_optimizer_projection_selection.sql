SET enable_optimizer=1;
SET optimizer_projection_support=1;

DROP TABLE IF EXISTS test_proj_dist;
DROP TABLE IF EXISTS test_proj;

CREATE TABLE test_proj
(
    `pkey` Int32,
    `okey` Int32,
    `key1` Int32,
    `val` Int64
)
ENGINE = MergeTree
PARTITION BY pkey
ORDER BY okey;

CREATE TABLE test_proj_dist AS test_proj ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'test_proj');

-- group 1: 50 of 100 parts will be selected

INSERT INTO test_proj(pkey, okey, key1, val)
SELECT number / 10000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000;

-- group 2: 100 of 100 parts will be selected

ALTER TABLE test_proj ADD PROJECTION proj1
    (
SELECT
    pkey,
    key1,
    sum(val)
    GROUP BY pkey, key1
);

INSERT INTO test_proj(pkey, okey, key1, val)
SELECT number / 10000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000 OFFSET 1000000;

-- group 3: 30 of 100 parts will be selected

ALTER TABLE test_proj ADD COLUMN `key2` Int32 AFTER `key1`;
ALTER TABLE test_proj_dist ADD COLUMN `key2` Int32 AFTER `key1`;

INSERT INTO test_proj(pkey, okey, key1, val)
SELECT number / 10000, number % 10000, number % 2, 1
FROM system.numbers LIMIT 1000000 OFFSET 2000000;

SELECT key1, SUM(val) FROM test_proj_dist WHERE pkey >=50 AND pkey < 230 GROUP BY key1;

DROP TABLE IF EXISTS test_proj_dist;
DROP TABLE IF EXISTS test_proj;
