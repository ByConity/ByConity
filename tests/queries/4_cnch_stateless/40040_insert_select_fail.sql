set enable_optimizer = 1;
set enable_materialized_view_rewrite = 1;

create database if not exists test;

DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS source1;
DROP TABLE IF EXISTS target1;
DROP TABLE IF EXISTS temp;

CREATE TABLE source1 (app_id UInt32, server_time UInt64, event_name String, uid UInt64, cost UInt64, duration UInt64, event_date Date) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, uid, event_name);
CREATE TABLE target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64) , max_duration AggregateFunction(max, UInt64)) ENGINE = CnchMergeTree() PARTITION BY toDate(event_date) ORDER BY (app_id, event_name, event_date);
CREATE MATERIALIZED VIEW mv1 to target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64), max_duration AggregateFunction(max, UInt64))
AS SELECT
     app_id,
     event_name,
     event_date,
     sumState(cost) AS sum_cost,
     maxState(duration) AS max_duration
FROM source1
GROUP BY app_id, event_name, event_date;

CREATE TABLE temp (app_id UInt32, event_name String, event_date Date, cost UInt64, duration UInt64) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, event_name);

insert into temp select app_id, event_name, event_date, sum(cost), max(duration) from source1 group by app_id, event_name, event_date order by app_id;
