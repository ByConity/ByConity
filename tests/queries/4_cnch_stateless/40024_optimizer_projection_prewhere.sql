SET enable_optimizer=1;
SET optimizer_projection_support=1;
SET force_optimize_projection=1;
CREATE DATABASE IF NOT EXISTS test_proj_prewhere_db;
USE test_proj_prewhere_db;

DROP TABLE IF EXISTS test_proj_prewhere;

CREATE TABLE test_proj_prewhere(at UInt64, vid UInt64, system_origin Int32, clicks UInt64)
ENGINE = CnchMergeTree() PARTITION BY toDate(at) ORDER BY vid;

ALTER TABLE test_proj_prewhere ADD PROJECTION proj (SELECT toDate(at), vid, system_origin, sum(clicks) GROUP BY toDate(at), vid, system_origin);

INSERT INTO test_proj_prewhere
SELECT 1641038400 + number AS at, 1001 AS vid, number % 2 == 0 ? 0 : 1 AS system_origin, 1 AS clicks
FROM system.numbers LIMIT 1000;

SELECT
    toDate(at), vid, sum(clicks)
FROM test_proj_prewhere
PREWHERE toDate(at) = '2022-01-01' AND vid = 1001
WHERE system_origin != 1
GROUP BY toDate(at), vid;
