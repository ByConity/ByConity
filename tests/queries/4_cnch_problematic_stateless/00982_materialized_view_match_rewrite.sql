

--- materialize view definition
DROP TABLE IF EXISTS aggregate_view;
DROP TABLE IF EXISTS aggregate_data;
DROP TABLE IF EXISTS event_metric;

--- destination aggregate table
CREATE TABLE aggregate_data
(
    `app_id` UInt32,
    `event_name` String,
    `event_date` Date,
    `sum_cost` AggregateFunction(sum, UInt64),
    `max_duration` AggregateFunction(max, UInt64)
)
ENGINE = CnchAggregatingMergeTree
PARTITION BY toDate(event_date)
ORDER BY (app_id, event_name, event_date)
SETTINGS index_granularity = 8192;

--- event metric cnch merge tree table
CREATE TABLE event_metric
(
    `app_id` UInt32,
    `server_time` UInt64,
    `event_name` String,
    `uid` UInt64,
    `cost` UInt64,
    `duration` UInt64,
    `event_date` Date
)
ENGINE = CnchMergeTree
PARTITION BY toDate(event_date)
ORDER BY (app_id, uid, event_name)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW aggregate_view TO aggregate_data
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
FROM event_metric
GROUP BY
    app_id,
    event_name,
    event_date;

--- insert into original table data
insert into table event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (1, now(), 'show', 121245, 44, 64, toDate('2021-11-29'));
insert into table event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (2, now(), 'send', 2345, 476, 64, toDate('2021-11-30'));
insert into table event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (3, now(), 'show', 544545, 87, 889, toDate('2021-11-30'));
insert into table event_metric(app_id, server_time, event_name, uid, cost, duration, event_date) values (4, now(), 'slide', 234545, 123, 98, toDate('2021-11-30'));

--- refresh commands
truncate table aggregate_data;
refresh materialized view aggregate_view partition '2021-11-30';
refresh materialized view aggregate_view partition '2021-11-29';

SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM event_metric GROUP BY app_id, event_name, event_date order by app_id settings enable_view_based_query_rewrite = 0;
SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM event_metric GROUP BY app_id, event_name, event_date order by app_id settings enable_view_based_query_rewrite = 1;

SELECT app_id, event_name, sum(cost) AS sum_cost FROM event_metric GROUP BY app_id, event_name order by app_id settings enable_view_based_query_rewrite = 0;
SELECT app_id, event_name, sum(cost) AS sum_cost FROM event_metric GROUP BY app_id, event_name order by app_id settings enable_view_based_query_rewrite = 1;

element SELECT app_id, event_name, event_date, sum(cost) AS sum_cost, max(duration) AS max_duration FROM event_metric GROUP BY app_id, event_name, event_date;

DROP TABLE aggregate_view;
DROP TABLE event_metric;
DROP TABLE aggregate_data;
