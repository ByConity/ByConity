SET enable_optimizer = 1;
SET enable_materialized_view_rewrite = 1;
SET enforce_materialized_view_rewrite = 1;
SET enable_optimizer_fallback=0;
SET enable_materialized_view_union_rewriting = 0;
SET materialized_view_consistency_check_method = 'PARTITION';
SET enable_optimizer_for_create_select = 0;

CREATE DATABASE IF NOT EXISTS test_40037;
USE test_40037;

DROP TABLE IF EXISTS mv40037;
DROP TABLE IF EXISTS base40037;
DROP TABLE IF EXISTS target40037;

CREATE TABLE base40037(server_time UInt64, event_date Date, uid String, click UInt64)
ENGINE = CnchMergeTree()
PARTITION BY (event_date, toHour(toDateTime(server_time, 'Europe/Moscow')))
ORDER BY (event_date, toHour(toDateTime(server_time, 'Europe/Moscow')));

INSERT INTO base40037 VALUES (1672549200, '2023-01-01', '1000', 1);  -- 2023-01-01T08:00:00
INSERT INTO base40037 VALUES (1672552800, '2023-01-01', '1000', 2);  -- 2023-01-01T09:00:00
INSERT INTO base40037 VALUES (1672635600, '2023-01-02', '1000', 3);  -- 2023-01-02T08:00:00

-- test_40037 mv
DROP TABLE IF EXISTS target40037;
DROP TABLE IF EXISTS mv40037;

CREATE TABLE target40037
ENGINE = CnchAggregatingMergeTree()
PARTITION BY (event_date, server_time_hour)
ORDER BY (event_date, server_time_hour) AS
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sumState(click) AS sum_click
FROM base40037
WHERE event_date = '2023-01-01' -- only 2023-01-01 data is populated
GROUP BY server_time_hour, event_date, uid;

CREATE MATERIALIZED VIEW mv40037 TO target40037
AS SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sumState(click) AS sum_click
FROM base40037
WHERE event_date = '2023-01-01' 
GROUP BY server_time_hour, event_date, uid;

-- { echo }

-- not hit mv
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
GROUP BY server_time_hour, event_date, uid; -- { serverError 3011 }

-- hit mv
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE event_date = '2023-01-01'
GROUP BY server_time_hour, event_date, uid
ORDER BY server_time_hour, event_date, uid;

SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE event_date = '2023-01-01'
GROUP BY server_time_hour, event_date, uid
ORDER BY server_time_hour, event_date, uid
settings enable_materialized_view_rewrite=0;

-- not hit mv
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE toHour(toDateTime(server_time, 'Europe/Moscow')) = 8
GROUP BY server_time_hour, event_date, uid; -- { serverError 3011 }

-- not hit mv
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE event_date BETWEEN '2023-01-01' AND '2023-01-10'
GROUP BY server_time_hour, event_date, uid; -- { serverError 3011 }

-- hit mv
SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE toHour(toDateTime(server_time, 'Europe/Moscow')) IN (9, 10, 11) AND event_date = '2023-01-01'
GROUP BY server_time_hour, event_date, uid
ORDER BY server_time_hour, event_date, uid;

SELECT
    toHour(toDateTime(server_time, 'Europe/Moscow')) AS server_time_hour,
    event_date,
    uid,
    sum(click) AS sum_click
FROM base40037
WHERE toHour(toDateTime(server_time, 'Europe/Moscow')) IN (9, 10, 11) AND event_date = '2023-01-01'
GROUP BY server_time_hour, event_date, uid
ORDER BY server_time_hour, event_date, uid
settings enable_materialized_view_rewrite=0;

DROP TABLE IF EXISTS mv40037;
DROP TABLE IF EXISTS mv40037_2;
DROP TABLE IF EXISTS base40037;
DROP TABLE IF EXISTS target40037;
DROP TABLE IF EXISTS mv40037;
DROP DATABASE IF EXISTS test_40037;
