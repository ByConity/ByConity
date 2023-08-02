SET enable_optimizer=1;
SET optimizer_projection_support=1;

DROP TABLE IF EXISTS test_proj_dist;
DROP TABLE IF EXISTS test_proj;

CREATE TABLE test_proj
(
    `part` Int32,
    `key` Int32,
    `unused1` Int32,
    `unused2` Int32,
    `unused3` Int32,
    `val` Int64
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY key
SETTINGS index_granularity = 1000;

CREATE TABLE test_proj_dist AS test_proj ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'test_proj');

INSERT INTO test_proj(part, key, val)
SELECT
    number / 100000,
    0 + (number % 10),
    1
FROM system.numbers LIMIT 100000;

ALTER TABLE test_proj ADD PROJECTION unused_proj1
(
SELECT
    unused1,
    sum(val)
GROUP BY unused1
);

INSERT INTO test_proj(part, key, val)
SELECT
    number / 100000,
    10 + (number % 10),
    1
FROM system.numbers LIMIT 100000 OFFSET 100000;

ALTER TABLE test_proj ADD PROJECTION unused_proj2
(
SELECT
    unused2,
    sum(val)
    GROUP BY unused2
);

ALTER TABLE test_proj ADD PROJECTION proj1
(
SELECT
    key,
    sum(val)
    GROUP BY key
);

INSERT INTO test_proj(part, key, val)
SELECT
    number / 100000,
    20 + (number % 10),
    1
FROM system.numbers LIMIT 100000 OFFSET 200000;

ALTER TABLE test_proj ADD PROJECTION unused_proj3
(
SELECT
    unused3,
    sum(val)
    GROUP BY unused3
);

INSERT INTO test_proj(part, key, val)
SELECT
    number / 100000,
    30 + (number % 10),
    1
FROM system.numbers LIMIT 100000 OFFSET 300000;

-- { echoOn }
-- normal part return empty result
SELECT key, sum(val)
FROM test_proj_dist
WHERE key >= 10
GROUP BY key
ORDER BY key;

-- projection part return empty result
SELECT key, sum(val)
FROM test_proj_dist
WHERE key < 30
GROUP BY key
ORDER BY key;

-- all parts return empty result except a normal part
SELECT key, sum(val)
FROM test_proj_dist
WHERE key < 10
GROUP BY key
ORDER BY key;

-- all parts return empty result except a projection part
SELECT key, sum(val)
FROM test_proj_dist
WHERE key >= 30
GROUP BY key
ORDER BY key;

-- all parts return empty result
SELECT key, sum(val)
FROM test_proj_dist
WHERE key < 0
GROUP BY key
ORDER BY key;

-- { echoOff }

DROP TABLE IF EXISTS test_proj_dist;
DROP TABLE IF EXISTS test_proj;

-- empty part issue 20230131
DROP TABLE IF EXISTS hive_ad_local_abtest_board_overwrite_daily_slice_8192_local;
DROP TABLE IF EXISTS hive_ad_local_abtest_board_overwrite_daily_slice_8192;

CREATE TABLE hive_ad_local_abtest_board_overwrite_daily_slice_8192_local
(
    `slice` String,
    `vid` Int64,
    `rit` String,
    `click` Int64,
    `pricing_type` String,
    `system_origin` String,
    `p_date` Date,
    PROJECTION daily_pro_agg_test_v1
        (
        SELECT
        sum(click)
        GROUP BY
        vid,
        system_origin,
        pricing_type,
        rit,
        p_date
        )
)
    ENGINE = MergeTree()
PARTITION BY p_date
ORDER BY (vid, system_origin, pricing_type, rit, cityHash64(slice))
SAMPLE BY cityHash64(slice)
TTL p_date + toIntervalDay(20)
SETTINGS index_granularity = 8192;


CREATE TABLE hive_ad_local_abtest_board_overwrite_daily_slice_8192
(
    `slice` String,
    `vid` Int64,
    `rit` String,
    `click` Int64,
    `pricing_type` String,
    `system_origin` String,
    `p_date` Date
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'hive_ad_local_abtest_board_overwrite_daily_slice_8192_local', cityHash64(slice));

SELECT
    p_date,
    vid AS _vid,
    sum(click) * 1. AS Bid_for_OCPM_OCPC_CPA__show
FROM hive_ad_local_abtest_board_overwrite_daily_slice_8192
    PREWHERE vid IN ('4724668', '4724669', '4724670', '4724671')
WHERE (p_date >= '2022-09-09') AND (p_date < '2022-09-16') AND (system_origin NOT IN ('1', '5', '6', '15', '17', '22', '26')) AND ((pricing_type = '7') OR (pricing_type = '8') OR (pricing_type = '9') OR (pricing_type = '11')) AND (rit IN ('80008'))
GROUP BY p_date, vid
    settings enable_optimizer = 1,enable_materialized_view_rewrite=0,optimizer_projection_support = 1;

DROP TABLE IF EXISTS hive_ad_local_abtest_board_overwrite_daily_slice_8192_local;
DROP TABLE IF EXISTS hive_ad_local_abtest_board_overwrite_daily_slice_8192;
