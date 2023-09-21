SET enable_optimizer = 1;
SET enable_materialized_view_rewrite = 1;
SET enable_materialized_view_rewrite_verbose_log = 1;
SET enable_materialized_view_rewrite_match_range_filter = 1;
set materialized_view_consistency_check_method='NONE';

CREATE DATABASE IF NOT EXISTS test;
USE test;

DROP TABLE IF EXISTS mv40045;
DROP TABLE IF EXISTS base40045;
DROP TABLE IF EXISTS target40045;

CREATE TABLE base40045(server_time UInt64, event_date Date, uid String, click UInt64)
ENGINE = CnchMergeTree()
PARTITION BY (event_date, toHour(toDateTime(server_time)))
ORDER BY tuple();

CREATE TABLE target40045(server_time_hour UInt8, event_date Date, uid String, sum_click AggregateFunction(sum, UInt64))
ENGINE = CnchAggregatingMergeTree()
PARTITION BY (event_date, server_time_hour)
ORDER BY tuple();

DROP TABLE IF EXISTS mv40045;

CREATE MATERIALIZED VIEW mv40045 TO target40045
AS SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sumState(click) AS sum_click
FROM base40045
WHERE event_date >= '2023-01-01'
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 1, expect hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE event_date = '2023-01-02'
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 2, expect hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE event_date > '2023-01-01'
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 3, expect not hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE event_date >= '2022-01-01'
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 4, expect not hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 5, expect hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE event_date IN ('2023-01-01', '2023-01-02')
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 6, expect hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE event_date IN ('2023-01-01', '2023-01-02') AND uid = 'xxx'
GROUP BY server_time_hour, event_date, uid;

DROP TABLE IF EXISTS mv40045;

CREATE MATERIALIZED VIEW mv40045 TO target40045
AS SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sumState(click) AS sum_click
FROM base40045
WHERE server_time_hour IN (0, 1, 2)
GROUP BY server_time_hour, event_date, uid;

SELECT 'case 7, expect hit mv';
EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40045
WHERE server_time_hour = 2
GROUP BY server_time_hour, event_date, uid;
