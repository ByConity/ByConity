drop database if exists mv10054_1;
create database mv10054_1;

CREATE TABLE mv10054_1.source1 (app_id UInt32, server_time UInt64, event_name String, uid UInt64, cost UInt64, duration UInt64, event_date Date) ENGINE = CnchMergeTree PARTITION BY toDate(event_date) ORDER BY (app_id, uid, event_name);
CREATE TABLE mv10054_1.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64) , max_duration AggregateFunction(max, UInt64)) ENGINE = CnchAggregatingMergeTree() PARTITION BY toDate(event_date) ORDER BY (app_id, event_name, event_date);

insert into table mv10054_1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (1, 1642149961, 'show', 100001, 1, 1, '2022-06-14');
insert into table mv10054_1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (2, 1642149961 , 'send', 100002, 2, 2, '2022-06-15');
insert into table mv10054_1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (1, 1642149961, 'show', 100001, 1, 1, '2022-06-17');
insert into table mv10054_1.source1(app_id, server_time, event_name, uid, cost, duration, event_date) values (2, 1642149961 , 'send', 100002, 2, 2, '2022-06-18');
select count(*)==4 as res from mv10054_1.source1;

CREATE MATERIALIZED VIEW mv10054_1.mv1 to mv10054_1.target1 (app_id UInt32,  event_name String, event_date Date, sum_cost AggregateFunction(sum, UInt64), max_duration AggregateFunction(max, UInt64)) 
AS SELECT
     app_id,
     event_name,
     event_date,
     sumState(cost) AS sum_cost,
     maxState(duration) AS max_duration
FROM mv10054_1.source1
GROUP BY app_id, event_name, event_date; 
select count(*)==0 as res from mv10054_1.target1;

refresh materialized view mv10054_1.mv1 where toDate(event_date) < '2022-06-14';
select app_id, event_name, event_date, sumMerge(sum_cost), maxMerge(max_duration) from mv10054_1.mv1 group by app_id, event_name, event_date order by app_id;

refresh materialized view mv10054_1.mv1 where toDate(event_date) < '2022-06-16';
select app_id, event_name, event_date, sumMerge(sum_cost), maxMerge(max_duration) from mv10054_1.mv1 group by app_id, event_name, event_date order by app_id;

refresh materialized view mv10054_1.mv1 where toDate(event_date) < '2022-06-19' settings max_threads_to_refresh_by_partition=2;
select app_id, event_name, event_date, sumMerge(sum_cost), maxMerge(max_duration) from mv10054_1.mv1 group by app_id, event_name, event_date order by app_id, event_name, event_date;

drop database mv10054_1;