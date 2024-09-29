CREATE DATABASE IF NOT EXISTS 48066_test;
use 48066_test;
DROP TABLE IF EXISTS dm_union_monetization_cockpit_business_stats;
DROP TABLE IF EXISTS dm_union_ug_rta2_strategy_cost_daily;

CREATE TABLE dm_union_monetization_cockpit_business_stats
(
    customer_id Int64,
    `send_count` Nullable(Int64),
    `p_date` Date,
    `rat2` Nullable(Float64)
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY customer_id
SETTINGS index_granularity = 8192;

CREATE TABLE dm_union_ug_rta2_strategy_cost_daily
(
    slice_id Int64,
    `p_date` Date,
    `rat2` Nullable(Float64)
)
ENGINE = CnchMergeTree()
PARTITION BY p_date
ORDER BY (slice_id, cityHash64(slice_id))
SETTINGS index_granularity = 8192;

set dialect_type='CLICKHOUSE';
set enable_optimizer=1;
SELECT
    p_date AS _1700039538449,
    sum(`rat2[txt]`) AS _sum_1700061658141,
    sum(send_count) AS _sum_1700039538498
FROM
(
    SELECT
        send_count AS send_count,
        p_date AS p_date,
        rat2 AS rat2,
        NULL AS `rat2[txt]`
    FROM 48066_test.dm_union_monetization_cockpit_business_stats
    UNION ALL
    SELECT
        NULL AS send_count,
        p_date AS p_date,
        NULL AS rat2,
        rat2 AS `rat2[txt]`
    FROM 48066_test.dm_union_ug_rta2_strategy_cost_daily
)
GROUP BY p_date;