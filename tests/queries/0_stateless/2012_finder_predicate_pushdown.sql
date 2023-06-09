create database if not exists uba_testing_default;
drop table if exists uba_testing_default.events;
drop table if exists uba_testing_default.users;


CREATE TABLE uba_testing_default.events (
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
    ) ENGINE = MergeTree PARTITION BY (app_id, event_date)
ORDER BY
    (event, hash_uid, time) SAMPLE BY hash_uid TTL toDate(event_date) + toIntervalDay(750) SETTINGS index_granularity = 8192;
    

CREATE TABLE IF NOT EXISTS uba_testing_default.users (
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
    ) ENGINE = HaUniqueMergeTree(
        '/clickhouse/2100203dfs761/users/{shard}',
        '{replica}'
    )
ORDER BY
    (app_id, hash_uid) SAMPLE BY hash_uid UNIQUE KEY (app_id, hash_uid) SETTINGS index_granularity = 8192;


SELECT
    (
        toUInt32((toUInt64(time / 1000) - 1677600000) / 86400)
    ) as t,
    count(distinct et.hash_uid) as cntq00
FROM
    uba_testing_default.events et global any
    inner join (
        select
            hash_uid as join_key,
            toInt64(
                assumeNotNull(int_profiles { 'first_event_time' }) / 1000
            ) as first_event_time
        from
            uba_testing_default.users
        where
            app_id = 20000016
            and last_active_date >= '2023-03-01'
    ) upt on et.hash_uid = upt.join_key
WHERE
    app_id = 20000016
    and (
        (
            event not in ('rangers_push_send', 'rangers_push_workflow')
            and (
                ifNull(string_params { '$inactive' }, 'null') != 'true'
            )
        )
    )
    and (
        event_date >= '2023-03-01'
        and event_date <= '2023-03-01'
    )
    and (
        toDate(toDateTime(first_event_time), 'Asia/Shanghai') = toDate(
            toDateTime(toUInt64(time / 1000)),
            'Asia/Shanghai'
        )
    )
    and ((1 = 1))
group by
    t
LIMIT
    500000;


SELECT
    (
        toUInt32((toUInt64(time / 1000) - 1678809600) / 604800)
    ) as t,
    ifNull(string_params { 'path' }, 'null') as g0,
    count(1) as cntq00
FROM
    uba_testing_default.events et global any
    inner join (
        select
            hash_uid as join_key,
            toInt64(
                assumeNotNull(int_profiles { 'first_event_time' }) / 1000
            ) as first_event_time
        from
            uba_testing_default.users
        where
            app_id = 20000016
            and last_active_date >= '2023-03-15'
    ) upt on et.hash_uid = upt.join_key
WHERE
    app_id = 20000016
    and ((event = 'app_launch'))
    and (
        event_date >= '2023-03-15'
        and event_date <= '2023-03-21'
    )
    and (
        (
            ifNull(
                if(
                    isNotNull(string_params { 'mp_platform' }),
                    string_params { 'mp_platform' },
                    string_profiles { 'custom_mp_platform' }
                ),
                'null'
            ) in ('2')
        )
        and (
            toUInt32((first_event_time - 1678809600) / 604800) != toUInt32((toUInt64(time / 1000) - 1678809600) / 604800)
            or first_event_time < 1678809600
        )
        and (
            ifNull(string_params { 'platform' }, 'null') in ('mp')
        )
    )
    and ((1 = 1))
GROUP BY
    t,
    g0 TEALIMIT 1000 GROUP assumeNotNull(g0) ORDER cntq00 desc;


SELECT
    (
        toUInt32((toUInt64(time / 1000) - 1678723200) / 604800)
    ) as t,
    ifNull(string_params { 'url' }, 'null') as g0,
    count(distinct int_params { '_ip_int' }) as cntq00
FROM
    uba_testing_default.events et global any
    inner join (
        select
            hash_uid as join_key,
            toInt64(
                assumeNotNull(int_profiles { 'first_event_time' }) / 1000
            ) as first_event_time
        from
            uba_testing_default.users
        where
            app_id = 20000016
            and last_active_date >= '2023-03-14'
    ) upt on et.hash_uid = upt.join_key
WHERE
    app_id = 20000016
    and ((event = 'predefine_pageview'))
    and (
        event_date >= '2023-03-14'
        and event_date <= '2023-03-20'
    )
    and (
        (
            ifNull(string_params { 'referer_type' }, 'null') in ('direct')
        )
    )
    and (
        (
            toUInt32((first_event_time - 1678723200) / 604800) = toUInt32((toUInt64(time / 1000) - 1678723200) / 604800)
            and first_event_time >= 1678723200
        )
        and (
            ifNull(string_params { 'platform' }, 'null') in ('wap', 'web')
        )
    )
    and ((1 = 1))
GROUP BY
    t,
    g0 TEALIMIT 50 GROUP assumeNotNull(g0) ORDER cntq00 desc;


SELECT
    (
        toUInt32((toUInt64(time / 1000) - 1678809600) / 604800)
    ) as t,
    int_params { 'query_code_id' } as g0,
    count(distinct et.hash_uid) as cntq00
FROM
    uba_testing_default.events et global any
    inner join (
        select
            hash_uid as join_key,
            toInt64(
                assumeNotNull(int_profiles { 'first_event_time' }) / 1000
            ) as first_event_time
        from
            uba_testing_default.users
        where
            app_id = 20000016
            and last_active_date >= '2023-03-15'
    ) upt on et.hash_uid = upt.join_key
WHERE
    app_id = 20000016
    and ((event = 'app_launch'))
    and (
        event_date >= '2023-03-15'
        and event_date <= '2023-03-21'
    )
    and (
        (
            toUInt32((first_event_time - 1678809600) / 604800) = toUInt32((toUInt64(time / 1000) - 1678809600) / 604800)
            and first_event_time >= 1678809600
        )
        and (int_params { 'query_code_id' } is not null)
        and (
            int_params { 'query_code_id' } in (
                100000010,
                100000001,
                100000002,
                100000003,
                100000004,
                100000005,
                100000006,
                100000007,
                100000008,
                100000009
            )
        )
    )
    and (
        (
            ifNull(
                if(
                    isNotNull(string_params { 'mp_platform' }),
                    string_params { 'mp_platform' },
                    string_profiles { 'custom_mp_platform' }
                ),
                'null'
            ) in ('1')
        )
        and (
            ifNull(string_params { 'platform' }, 'null') in ('mp')
        )
    )
    and ((1 = 1))
GROUP BY
    t,
    g0 TEALIMIT 1000 GROUP assumeNotNull(g0) ORDER cntq00 desc;


SELECT
    (
        toUInt32((toUInt64(time / 1000) - 1678809600) / 86400)
    ) as t,
    count(distinct et.hash_uid) as cntq00
FROM
    uba_testing_default.events et global any
    inner join (
        select
            hash_uid as join_key,
            toInt64(
                assumeNotNull(int_profiles { 'first_event_time' }) / 1000
            ) as first_event_time
        from
            uba_testing_default.users
        where
            app_id = 20000016
            and last_active_date >= '2023-03-15'
    ) upt on et.hash_uid = upt.join_key
WHERE
    app_id = 20000016
    and (
        (
            event not in ('rangers_push_send', 'rangers_push_workflow')
            and (
                ifNull(string_params { '$inactive' }, 'null') != 'true'
            )
        )
    )
    and (
        event_date >= '2023-03-15'
        and event_date <= '2023-03-21'
    )
    and (
        toDate(toDateTime(first_event_time), 'Asia/Shanghai') = toDate(toDateTime(toUInt64(time / 1000)), 'Asia/Shanghai')
    )
    and ((1 = 1))
group by
    t
LIMIT
    500000;