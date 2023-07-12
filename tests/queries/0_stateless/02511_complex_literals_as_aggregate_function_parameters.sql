SELECT toTypeName(sumMapFilteredState([1, 2])([1, 2, 3], [10, 10, 10]));
SELECT hex(sumMapFilteredState([1, 2])([1, 2, 3], [10, 10, 10]));
SELECT hex(unhex('02010A00000000000000020A00000000000000')::AggregateFunction(sumMapFiltered([1, 2]), Array(UInt8), Array(UInt8)));

DROP TABLE if exists tob_apps;
DROP TABLE if exists tob_apps_all;

CREATE TABLE if not exists tob_apps
(
    `tea_app_id` UInt32,
    `event` String,
    `time` UInt64,
    `event_date` Date,
    `user_id` String DEFAULT '',
    `server_time` UInt64,
    `hash_uid` UInt64,
    `user_unique_id` String,
    `dummy` Int64
)
ENGINE = MergeTree
PARTITION BY (tea_app_id, event_date)
ORDER BY (tea_app_id, event, event_date, hash_uid, user_unique_id);

create table if not exists tob_apps_all as tob_apps ENGINE=Distributed('test_shard_localhost', currentDatabase(), 'tob_apps');

SELECT funnelRep2(1, 2, 0, [0, 60000, 120000, 180000, 240000, 300000, 360000, 420000, 480000, 540000, 600000])(funnel_tmp_res.1, funnel_tmp_res.2) AS col2
FROM
(
    SELECT finderFunnel(600000, 1682956800, 345600, 1, 0, 0, 'Asia/Shanghai', 1)(multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time), multiIf(time <= 2000000000, time * 1000, time), unifyNull(event = 'predefine_pageview'), unifyNull(event = 'predefine_pageview')) AS funnel_tmp_res
    FROM tob_apps_all AS et
    WHERE (tea_app_id = 41514) AND ((event = 'predefine_pageview') OR (event = 'predefine_pageview')) AND ((event_date >= '2023-05-02') AND (event_date <= '2023-05-06') AND (multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) >= 1682956800) AND (multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) <= 1683302999))
    GROUP BY et.hash_uid
);

DROP TABLE if exists tob_apps;
DROP TABLE if exists tob_apps_all;