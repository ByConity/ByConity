DROP TABLE IF EXISTS ut40068_apps;

CREATE TABLE ut40068_apps
(
    `hash_uid` UInt64,
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `user_unique_id` String,
    `event_date` Date,
    `tea_app_id` UInt32
)
ENGINE = CnchMergeTree()
PARTITION BY (tea_app_id, event_date)
ORDER BY (tea_app_id, event, event_date, hash_uid, user_unique_id);

INSERT INTO ut40068_apps
SELECT
    number AS hash_uid,
    1606406500 AS server_time,
    1606406500 AS time,
    'xx' AS event,
    'yy' AS user_unique_id,
    '2021-03-21' AS event_date,
    41514 AS tea_app_id
FROM system.numbers limit 10;

EXPLAIN
SELECT
    toUInt32((multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) - 1676304000) / 604800) AS t,
    1 AS g0,
    count(et.hash_uid) AS cntq00
FROM ut40068_apps AS et
WHERE ((tea_app_id = 41514) AND (1 = 1)) AND ((event_date >= '2021-03-21') AND (event_date <= '2023-08-23'))
    AND (multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) >= 1616304000)
    AND (multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) <= 1696563199)
GROUP BY
    t,
    g0
SETTINGS enable_optimizer = 1, enable_partition_filter_push_down = 1, enable_optimizer_early_prewhere_push_down = 1, enable_common_expression_sharing = 1, enable_common_expression_sharing_for_prewhere = 1;
