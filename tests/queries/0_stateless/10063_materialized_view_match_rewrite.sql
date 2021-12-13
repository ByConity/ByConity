USE test;

--- event metric merge tree local table
DROP TABLE IF EXISTS event_metric_local;
CREATE TABLE test.event_metric_local
(
    `app_id` UInt32,
    `server_time` UInt64,
    `event_name` String,
    `uid` UInt64,
    `cost` UInt64,
    `duration` UInt64,
    `event_date` Date
)
ENGINE = MergeTree
PARTITION BY toDate(event_date)
ORDER BY (app_id, uid, event_name)
SETTINGS index_granularity = 8192;

--- original distribute table
DROP TABLE IF EXISTS event_metric;
create table test.event_metric (
  `app_id` UInt32,
  `server_time` UInt64,
  `event_name` String,
  `uid` UInt64,
  `cost` UInt64,
  `duration` UInt64,
  `event_date` Date
) ENGINE = Distributed(
  test_shard_localhost,
  test,
  event_metric_local
);

--- insert into original table data
insert into table test.event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (1, now(), 'show', 121245, 44, 64, toDate('2021-11-30'));
insert into table test.event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (2, now(), 'send', 2345, 476, 64, toDate('2021-11-30'));
insert into table test.event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (3, now(), 'show', 544545, 87, 889, toDate('2021-11-30'));
insert into table test.event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (4, now(), 'slide', 234545, 123, 98, toDate('2021-11-30'));

--- destination aggregate table
DROP TABLE IF EXISTS test.aggregate_data_local;
CREATE TABLE test.aggregate_data_local
(
    `app_id` UInt32,
    `event_name` String,
    `event_date` Date,
    `sum_cost` AggregateFunction(sum, UInt64),
    `max_duration` AggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(event_date)
ORDER BY (app_id, event_name, event_date)
SETTINGS index_granularity = 8192;

--- materialized view distribute table
DROP TABLE IF EXISTS aggregate_data;
create table test.aggregate_data (
    `app_id` UInt32,
    `event_name` String,
    `event_date` Date,
    `sum_cost` AggregateFunction(sum, UInt64),
    `max_duration` AggregateFunction(max, UInt64)
) ENGINE = Distributed(
  test_shard_localhost,
  test,
  aggregate_data_local
);

--- materialize view definition
DROP TABLE IF EXISTS test.aggregate_view;
CREATE MATERIALIZED VIEW test.aggregate_view TO test.aggregate_data_local 
(
    `app_id` UInt32,
    `event_name` String,
    `event_date` Date,
    `sum_cost` AggregateFunction(sum, UInt64),
    `max_duration` AggregateFunction(max, UInt64)
) AS
SELECT
    app_id,
    event_name,
    event_date,
    sumState(cost) AS sum_cost,
    maxState(duration) AS max_duration
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name,
    event_date;

--- refresh command
REFRESH MATERIALIZED VIEW test.aggregate_view;
select app_id, max(duration) from test.event_metric_local group by app_id settings enable_view_based_query_rewrite = 1;

REFRESH MATERIALIZED VIEW test.aggregate_view PARTITION '2021-11-30';
select app_id, max(duration) from test.event_metric_local group by app_id settings enable_view_based_query_rewrite = 1;

--- local explain view 
EXPLAIN VIEW
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name,
    event_date;

--- local explian element
EXPLAIN ELEMENT
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name,
    event_date;

--- distribute explain view 
EXPLAIN VIEW
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric
GROUP BY
    app_id,
    event_name,
    event_date;

--- distribute explian element
EXPLAIN ELEMENT
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric
GROUP BY
    app_id,
    event_name,
    event_date;

--- local query rewrite case 1
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name,
    event_date settings enable_view_based_query_rewrite = 0;


SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name,
    event_date settings enable_view_based_query_rewrite = 1;

--- distribute query rewrite case 1
SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric
GROUP BY
    app_id,
    event_name,
    event_date settings enable_view_based_query_rewrite = 0;


SELECT
    app_id,
    event_name,
    event_date,
    sum(cost) AS sum_cost,
    max(duration) AS max_duration
FROM test.event_metric
GROUP BY
    app_id,
    event_name,
    event_date settings enable_view_based_query_rewrite = 1;

--- local query rewrite case 2
SELECT
    app_id,
    event_name,
    sum(cost) AS sum_cost
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name settings enable_view_based_query_rewrite = 0;

SELECT
    app_id,
    event_name,
    sum(cost) AS sum_cost
FROM test.event_metric_local
GROUP BY
    app_id,
    event_name settings enable_view_based_query_rewrite = 1;

--- distribute query rewrite case 2
SELECT
    app_id,
    event_name,
    sum(cost) AS sum_cost
FROM test.event_metric
GROUP BY
    app_id,
    event_name settings enable_view_based_query_rewrite = 0;

SELECT
    app_id,
    event_name,
    sum(cost) AS sum_cost
FROM test.event_metric
GROUP BY
    app_id,
    event_name settings enable_view_based_query_rewrite = 1;

DROP TABLE event_metric_local;
DROP TABLE event_metric;
DROP TABLE aggregate_data_local;
DROP TABLE aggregate_data;
DROP TABLE aggregate_view;
