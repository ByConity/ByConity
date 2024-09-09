create database if not exists uba_production_default;
use uba_production_default;
drop table if exists uba_production_default.events;
drop table if exists uba_production_default.users;
set enable_optimizer=1;

CREATE TABLE uba_production_default.events
(
    `app_id` UInt32,
    `app_name` String,
    `device_id` String,
    `web_id` String,
    `hash_uid` UInt64,
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `stat_standard_id` String,
    `event_date` Date,
    `string_params` Map(String, String),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_profiles` Map(String, String),
    `int_profiles` Map(String, Int64),
    `float_profiles` Map(String, Float64),
    `user_id` String,
    `ssid` String,
    `content` String,
    `string_array_profiles` Map(String, Array(String)),
    `string_array_params` Map(String, Array(String)),
    `string_item_profiles` Map(String, Array(String)),
    `float_item_profiles` Map(String, Array(Float32)),
    `int_item_profiles` Map(String, Array(Int64))
)
ENGINE = CnchMergeTree
PARTITION BY (app_id, event_date)
ORDER BY (event, hash_uid, time)
SETTINGS index_granularity = 8192;

CREATE TABLE uba_production_default.users
(
    `app_id` UInt32,
    `ssid` String DEFAULT '',
    `stat_standard_id` String DEFAULT '',
    `device_id` String DEFAULT '',
    `web_id` String DEFAULT '',
    `user_id` String DEFAULT '',
    `hash_uid` UInt64,
    `update_time` UInt64,
    `last_active_date` Date,
    `string_profiles` Map(String, String),
    `int_profiles` Map(String, Int64),
    `float_profiles` Map(String, Float64),
    `string_array_profiles` Map(String, Array(String))
)
ENGINE = CnchMergeTree
ORDER BY (app_id, hash_uid)
SAMPLE BY hash_uid
SETTINGS index_granularity = 8192;

SET enable_nested_loop_join = 0;
SELECT
    *
FROM uba_production_default.events AS et
GLOBAL ANY INNER JOIN
(
    SELECT
        hash_uid AS join_key,
        toInt64(assumeNotNull(int_profiles{'first_event_time'}) / 1000) AS first_event_time
    FROM uba_production_default.users
    WHERE (app_id = 20000696) AND (last_active_date >= '2023-03-15')
) AS upt ON et.hash_uid = upt.join_key
WHERE toDate(toDateTime(first_event_time), 'Asia/Shanghai') != toDate(toDateTime(toUInt64(time / 1000)), 'Asia/Shanghai');


SET enable_nested_loop_join = 1;
SELECT
    *
FROM uba_production_default.events AS et
GLOBAL ANY INNER JOIN
(
    SELECT
        hash_uid AS join_key,
        toInt64(assumeNotNull(int_profiles{'first_event_time'}) / 1000) AS first_event_time
    FROM uba_production_default.users
    WHERE (app_id = 20000696) AND (last_active_date >= '2023-03-15')
) AS upt ON et.hash_uid = upt.join_key
WHERE toDate(toDateTime(first_event_time), 'Asia/Shanghai') != toDate(toDateTime(toUInt64(time / 1000)), 'Asia/Shanghai');

EXPLAIN 
SELECT
    *
FROM uba_production_default.events AS et
GLOBAL ANY INNER JOIN
(
    SELECT
        hash_uid AS join_key,
        toInt64(assumeNotNull(int_profiles{'first_event_time'}) / 1000) AS first_event_time
    FROM uba_production_default.users
    WHERE (app_id = 20000696) AND (last_active_date >= '2023-03-15')
) AS upt ON et.hash_uid = upt.join_key
WHERE toDate(toDateTime(first_event_time), 'Asia/Shanghai') != toDate(toDateTime(toUInt64(time / 1000)), 'Asia/Shanghai');

drop table if exists uba_production_default.events;
drop table if exists uba_production_default.users;
create database if not exists uba_production_default;
