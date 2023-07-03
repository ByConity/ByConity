drop table if exists test.events_12345;
drop table if exists test.events_12345_all;

CREATE TABLE IF NOT EXISTS test.events_12345
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
    `ab_version` Array(Int32) BLOOM,
    `string_params` Map(String, LowCardinality(Nullable(String))),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_profiles` Map(String, LowCardinality(Nullable(String))),
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
ENGINE = MergeTree
PARTITION BY (app_id, event_date)
ORDER BY (event, hash_uid, time)
SAMPLE BY hash_uid
TTL toDate(event_date) + toIntervalDay(750)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS test.events_12345_all
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
    `ab_version` Array(Int32) BLOOM,
    `string_params` Map(String, LowCardinality(Nullable(String))),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_profiles` Map(String, LowCardinality(Nullable(String))),
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
ENGINE = Distributed('test_shard_localhost', 'test', 'events_12345');

select
    assumeNotNull(return_events) as _retention
from
    (
        select
            et.hash_uid as _subject,
            genArrayIf(1, 1678118400, 86400)(toUInt64(time / 1000), 1) as first_events
        from
            test.events_12345_all et
        group by
            _subject
    ) a any
    left join (
        select
            et.hash_uid as _subject,
            genArrayIf(1, 1678118400, 86400)(toUInt64(time / 1000), 1) as return_events
        from
            test.events_12345_all et
        group by
            _subject
    ) b on a._subject = b._subject
order by
    _retention desc
LIMIT
    50000 SETTINGS distributed_perfect_shard = 1, enable_optimizer = 0;

drop table if exists test.events_12345;
drop table if exists test.events_12345_all;
