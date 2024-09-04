set enable_optimizer = 1;
set enable_materialized_view_rewrite = 1;

create database if not exists test;

DROP TABLE IF EXISTS test.mv1;
DROP TABLE IF EXISTS test.source1;
DROP TABLE IF EXISTS test.target1;
DROP TABLE IF EXISTS test.temp;

CREATE TABLE test.source1 (app_id UInt32, server_time UInt64, event_name String, uid UInt64, cost UInt64, duration UInt64, event_date Date) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, uid, event_name);
CREATE TABLE test.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64) , max_duration AggregateFunction(max, UInt64)) ENGINE = CnchMergeTree() PARTITION BY toDate(event_date) ORDER BY (app_id, event_name, event_date);
CREATE MATERIALIZED VIEW test.mv1 to test.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64), max_duration AggregateFunction(max, UInt64))
AS SELECT
     app_id,
     event_name,
     event_date,
     sumState(cost) AS sum_cost,
     maxState(duration) AS max_duration
FROM test.source1
GROUP BY app_id, event_name, event_date;

CREATE TABLE test.temp (app_id UInt32, event_name String, event_date Date, cost UInt64, duration UInt64) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, event_name);

insert into test.temp select app_id, event_name, event_date, sum(cost), max(duration) from test.source1 group by app_id, event_name, event_date order by app_id;
