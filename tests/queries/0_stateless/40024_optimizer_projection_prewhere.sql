SET enable_optimizer=1;
SET optimizer_projection_support=1;

DROP TABLE IF EXISTS test_proj_dist;
DROP TABLE IF EXISTS test_proj;

CREATE TABLE test_proj(at UInt64, vid UInt64, system_origin Int32, clicks UInt64) ENGINE = MergeTree() PARTITION BY toDate(at) ORDER BY vid;
CREATE TABLE test_proj_dist AS test_proj ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'test_proj');

ALTER TABLE test_proj ADD PROJECTION proj (SELECT toDate(at), vid, system_origin, sum(clicks) GROUP BY toDate(at), vid, system_origin);

INSERT INTO test_proj
SELECT 1641038400 + number AS at, 1001 AS vid, number % 2 == 0 ? 0 : 1 AS system_origin, 1 AS clicks
FROM system.numbers LIMIT 1000;

SELECT
    toDate(at), vid, sum(clicks)
FROM test_proj_dist
PREWHERE toDate(at) = '2022-01-01' AND vid = 1001
WHERE system_origin != 1
GROUP BY toDate(at), vid;
