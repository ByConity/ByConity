SET enable_optimizer = 1;
SET enable_materialized_view_rewrite = 1;
SET enable_materialized_view_rewrite_verbose_log = 1;
set materialized_view_consistency_check_method='NONE';

CREATE DATABASE IF NOT EXISTS test;
USE test;

DROP TABLE IF EXISTS mv40051;
DROP TABLE IF EXISTS base40051;
DROP TABLE IF EXISTS target40051;

CREATE TABLE base40051(server_time UInt64, event_date Date, uid String, event_type UInt8, click UInt64)
ENGINE = CnchMergeTree()
PARTITION BY (event_date, toHour(toDateTime(server_time)))
ORDER BY uid;

CREATE TABLE target40051(server_time_hour UInt8, event_date Date, uid String, event_type UInt8, sum_click AggregateFunction(sum, UInt64))
ENGINE = CnchAggregatingMergeTree()
PARTITION BY (event_date, server_time_hour)
ORDER BY uid;

CREATE MATERIALIZED VIEW mv40051 TO target40051
AS SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    event_type,
    sumState(click) AS sum_click
FROM base40051
GROUP BY server_time_hour, event_date, uid, event_type;

EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    event_type,
    sum(click) AS sum_click
FROM base40051
PREWHERE uid = 'xx'
GROUP BY server_time_hour, event_date, uid, event_type;

EXPLAIN SELECT
    toHour(toDateTime(server_time)) AS server_time_hour,
    event_date,
    uid,
    event_type,
    sum(click) AS sum_click
FROM base40051
PREWHERE event_type = 1
GROUP BY server_time_hour, event_date, uid, event_type;
