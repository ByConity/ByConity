CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS chandler_trace_json_tp;

CREATE TABLE chandler_trace_json_tp
(
    `hour` UInt8,
    `dump_timestamp` UInt64,
    `trace_id` String,
    `address` String,
    `timestamp` UInt64,
    `service_name` String,
    `dc` String,
    `event_name` String,
    `event_date` String,
    `app_id` Int32,
    `vid_int` Array(Int32) BitMapIndex,
    `int_params` Map(String, Int32),
    `long_params` Map(String, Int64),
    `float_params` Map(String, Float32),
    `double_params` Map(String, Float64),
    `string_params` Map(String, String),
    `int_list_params` Map(String, Array(Int32)),
    `long_list_params` Map(String, Array(Int64)),
    `float_list_params` Map(String, Array(Float32)),
    `double_list_params` Map(String, Array(Float64)),
    `string_list_params` Map(String, Array(String))
) ENGINE = CnchMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service_name, event_name, hour, cityHash64(hour));

INSERT INTO chandler_trace_json_tp(timestamp, string_list_params) VALUES (1672617600, {'r_reasons': ['vstay', 'foo']});
INSERT INTO chandler_trace_json_tp(timestamp, string_list_params) VALUES (1672617600, {'r_reasons': ['foo', 'bar']});
INSERT INTO chandler_trace_json_tp(timestamp, string_list_params) VALUES (1672617600, {'r_reasons_1': ['foo', 'bar']});


SELECT DISTINCT string_list_params{'r_reasons'} AS _1700017401252_c617e8f29281f4e100741e5bc52f3eb6
FROM `test`.`chandler_trace_json_tp`
WHERE (arraySetCheck(assumeNotNull(string_list_params{'r_reasons'}), 'vstay') = 0)
  AND ((toDate(timestamp) >= '2022-12-31')
  AND (toDate(timestamp) <= '2023-01-06'))
order by _1700017401252_c617e8f29281f4e100741e5bc52f3eb6
LIMIT 1000
SETTINGS enable_optimizer = 1;

DROP TABLE IF EXISTS chandler_trace_json_tp;
