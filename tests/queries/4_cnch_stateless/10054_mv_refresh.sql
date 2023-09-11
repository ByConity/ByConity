drop database if exists mv1;
create database mv1;

CREATE TABLE mv1.source1 (app_id UInt32, server_time UInt64, event_name String, uid UInt64, cost UInt64, duration UInt64, event_date Date) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, uid, event_name);
CREATE TABLE mv1.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64) , max_duration AggregateFunction(max, UInt64)) ENGINE = CnchAggregatingMergeTree() PARTITION BY toDate(event_date) ORDER BY (app_id, event_name, event_date);

insert into table mv1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (1, 1642149961, 'show', 100001, 1, 1, '2022-06-14');
insert into table mv1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (2, 1642149961 , 'send', 100002, 2, 2, '2022-06-14');
select count(*)==2 as res from mv1.source1;

CREATE MATERIALIZED VIEW mv1.mv1 to mv1.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64), max_duration AggregateFunction(max, UInt64)) 
AS SELECT
     app_id,
     event_name,
     event_date,
     sumState(cost) AS sum_cost,
     maxState(duration) AS max_duration
FROM mv1.source1
GROUP BY app_id, event_name, event_date; 
select count(*)==0 as res from mv1.target1;

refresh materialized view mv1.mv1 partition '2022-06-14';
select app_id, event_name, event_date, sumMerge(sum_cost), maxMerge(max_duration) from mv1.mv1 group by app_id, event_name, event_date order by app_id;