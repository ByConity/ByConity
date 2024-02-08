set enable_optimizer=1;
set enable_optimizer_fallback=0;

create database if not exists test;

drop table if exists test.test_fusion_merge_history;
drop table if exists test.test_fusion_merge_real;

CREATE TABLE test.test_fusion_merge_history(`event_date` Date, `server_time` UInt64, `name` String) ENGINE = CnchMergeTree PARTITION BY event_date ORDER BY event_date SETTINGS index_granularity = 8192;
CREATE TABLE test.test_fusion_merge_real(`event_date` Date, `server_time` UInt64, `name` String) ENGINE = CnchMergeTree PARTITION BY event_date ORDER BY event_date SETTINGS index_granularity = 8192;

insert into table test.test_fusion_merge_history values ('2020-01-01', 1577854800, 'test_fusion_merge_history1');
insert into table test.test_fusion_merge_history values ('2020-01-02', 1577944800, 'test_fusion_merge_history2');
insert into table test.test_fusion_merge_real values ('2020-01-01', 1577854800, 'test_fusion_merge_real1');
insert into table test.test_fusion_merge_real values ('2020-01-02', 1577944800, 'test_fusion_merge_real2');

-- rewirte to: SELECT * FROM test.test_fusion_merge_history WHERE ((event_date >= '2020-01-01') AND (server_time >= 1577854800)) AND (event_date <= '2020-01-01') AND (server_time <= 1577858400) UNION ALL SELECT * FROM test.test_fusion_merge_real WHERE ((event_date >= '2020-01-02') AND (server_time >= 1577941200)) AND (event_date <= '2020-01-02') AND (server_time <= 1577944800)
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name;
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400)', '[1577941200, 1577944800)')  order by name;
-- SELECT * FROM test.test_fusion_merge_history UNION ALL SELECT * FROM test.test_fusion_merge_real WHERE ((event_date >= '2020-01-02') AND (server_time >= 1577941200)) AND (event_date <= '2020-01-02') AND (server_time <= 1577944800)
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[]', '[1577941200, 1577944800]') order by name;
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[]', '[]') order by name;
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') where name = 'test_fusion_merge_history1' order by name;
-- rewirte to: SELECT * FROM test.test_fusion_merge_history WHERE ((event_date >= '2020-01-01') AND (server_time >= 1577854800000)) AND (event_date <= '2020-01-01') AND (server_time <= 1577858400000) UNION ALL SELECT * FROM test.test_fusion_merge_real WHERE ((event_date >= '2020-01-02') AND (server_time >= 1577941200000)) AND (event_date <= '2020-01-02') AND (server_time <= 1577944800000)
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800000, 1577858400000]', '[1577941200000, 1577944800000]') order by name;
-- rewirte to: SELECT *, If(server_time <= 2000000000, server_time * 1000, server_time) AS time FROM test.test_fusion_merge_history WHERE ((event_date >= '2020-01-01') AND (time >= 1577854800000)) AND (event_date <= '2020-01-01') AND (time <= 1577858400000) UNION ALL SELECT *, If(server_time <= 2000000000, server_time * 1000, server_time) AS time FROM test.test_fusion_merge_real WHERE ((event_date >= '2020-01-02') AND (time >= 1577941200000)) AND (event_date <= '2020-01-02') AND (time <= 1577944800000)
explain select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, If(server_time <= 2000000000, server_time * 1000, server_time) AS time, '[1577854800000, 1577858400000]', '[1577941200000, 1577944800000]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577858400, 1577854800]', '[1577941200, 1577944800]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '(1577854800, 1577858400]', '[1577941200, 1577944800]')  order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800)') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[]', '[1577941200, 1577944800]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[]', '[]') order by name;

select name from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') where name = 'test_fusion_merge_history1' order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[]', '[]') where name = 'test_fusion_merge_history1' order by name;

select * from fusionMerge(test, test_fusion_merge_historyxxx, test_fusion_merge_real, event_date, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name; -- { serverError 60 }

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_datexxx, server_time, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name; -- { serverError 47 }

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_timexxx, '[1577854800, 1577858400]', '[1577941200, 1577944800]') order by name; -- { serverError 16 }

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, server_time, '[xxx,jj]', '[[dfa]]') order by name; -- { serverError 1001 }

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, If(server_time <= 2000000000, server_time * 1000, server_time) AS time, '[1577854800000, 1577858400000]', '[1577941200000, 1577944800000]') order by name;

select time from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, If(server_time <= 2000000000, server_time * 1000, server_time) AS time, '[1577854800000, 1577858400000]', '[1577941200000, 1577944800000]') order by name;

select * from fusionMerge(test, test_fusion_merge_history, test_fusion_merge_real, event_date, If(server_time <= 2000000000, server_time * 1000, server_time), '[1577854800000, 1577858400000]', '[1577941200000, 1577944800000]') order by name;

drop table if exists test.test_fusion_merge_history;
drop table if exists test.test_fusion_merge_real;

create database if not exists rangers;
drop table if exists rangers.tob_apps_realtime_all;
drop table if exists rangers.tob_apps_all;

CREATE TABLE rangers.tob_apps_realtime_all
(
    `app_id` UInt32,
    `app_name` String DEFAULT '',
    `app_version` String DEFAULT '',
    `device_id` String DEFAULT '',
    `device_model` String DEFAULT '',
    `device_brand` String DEFAULT '',
    `hash_uid` UInt64,
    `os_name` String DEFAULT '',
    `os_version` String DEFAULT '',
    `network_type` String DEFAULT '',
    `network_carrier` String DEFAULT '',
    `app_channel` String DEFAULT '',
    `user_register_ts` Nullable(UInt64),
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `user_unique_id` String,
    `event_date` Date,
    `ab_version` Array(Int32),
    `tea_app_id` UInt32,
    `app_region` String DEFAULT '',
    `region` String DEFAULT '',
    `app_language` String DEFAULT '',
    `language` String DEFAULT '',
    `gender` String DEFAULT '',
    `age` String DEFAULT '',
    `string_params` Map(String, String),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_profiles` Map(String, String),
    `int_profiles` Map(String, Int64),
    `float_profiles` Map(String, Float64),
    `string_item_profiles` Map(String, Array(String)),
    `int_item_profiles` Map(String, Array(Int64)),
    `float_item_profiles` Map(String, Array(Float64)),
    `content` String,
    `string_array_params` Map(String, Array(String)),
    `string_array_profiles` Map(String, Array(String))
) ENGINE = CnchMergeTree() ORDER BY tuple();

CREATE TABLE rangers.tob_apps_all
(
    `app_id` UInt32,
    `app_name` String DEFAULT '',
    `app_version` String DEFAULT '',
    `device_id` String DEFAULT '',
    `device_model` String DEFAULT '',
    `device_brand` String DEFAULT '',
    `hash_uid` UInt64,
    `os_name` String DEFAULT '',
    `os_version` String DEFAULT '',
    `network_type` String DEFAULT '',
    `network_carrier` String DEFAULT '',
    `app_channel` String DEFAULT '',
    `user_register_ts` Nullable(UInt64),
    `server_time` UInt64,
    `time` UInt64,
    `event` String,
    `user_unique_id` String,
    `event_date` Date,
    `ab_version` Array(Int32),
    `tea_app_id` UInt32,
    `app_region` String DEFAULT '',
    `region` String DEFAULT '',
    `app_language` String DEFAULT '',
    `language` String DEFAULT '',
    `gender` String DEFAULT '',
    `age` String DEFAULT '',
    `string_params` Map(String, String),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_profiles` Map(String, String),
    `int_profiles` Map(String, Int64),
    `float_profiles` Map(String, Float64),
    `user_id` String DEFAULT '',
    `ssid` String DEFAULT '',
    `string_item_profiles` Map(String, Array(String)),
    `int_item_profiles` Map(String, Array(Int64)),
    `float_item_profiles` Map(String, Array(Float64)),
    `content` String,
    `string_array_params` Map(String, Array(String)),
    `string_array_profiles` Map(String, Array(String))
) ENGINE = CnchMergeTree() ORDER BY tuple();

INSERT INTO rangers.tob_apps_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'hist', 1606400000, 1681747000);           -- not counted
INSERT INTO rangers.tob_apps_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'hist', 1606406400, 1681747000);           -- counted
INSERT INTO rangers.tob_apps_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'hist', 1606406400, 1681748000);           -- not counted
INSERT INTO rangers.tob_apps_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'hist', 1606406400, 1681747100000);        -- counted

INSERT INTO rangers.tob_apps_realtime_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'real', 1606400000, 1681833000);  -- not counted
INSERT INTO rangers.tob_apps_realtime_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'real', 1606406400, 1681747000);  -- not counted
INSERT INTO rangers.tob_apps_realtime_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'real', 1606406400, 1681833000);  -- counted
INSERT INTO rangers.tob_apps_realtime_all(tea_app_id, event_date, event, server_time, time) VALUES (41514, '2023-04-17', 'real', 1606406400, 1681834000);  -- not counted


SELECT
    toUInt32((multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time) - 1681142400) / 86400) AS t,
    ifNull(event, 'null') AS g0,
    count(1) AS cntq00
FROM fusionMerge(rangers, tob_apps_all, tob_apps_realtime_all, event_date, multiIf(server_time < 1606406400, server_time, time > 2000000000, toUInt32(time / 1000), time), '[1681142400,1681747199]', '(1681747199,1681833599]') AS et
WHERE (tea_app_id = 41514) AND (1 = 1)
GROUP BY
    t,
    g0
SETTINGS enable_optimizer = 1
TEALIMIT 50 GROUP assumeNotNull(g0) ORDER cntq00 DESC
FORMAT Null
SETTINGS enable_optimizer = 1;
