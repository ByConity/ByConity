drop table if exists delete_from_unique_table;

CREATE table delete_from_unique_table(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree()
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id;

insert into delete_from_unique_table values ('2021-07-13 18:50:00', 10001, 5, 500),('2021-07-13 18:50:00', 10002, 2, 200),('2021-07-13 18:50:00', 10003, 1, 100);
insert into delete_from_unique_table values ('2021-07-13 18:50:01', 10002, 4, 400),('2021-07-14 18:50:01', 10003, 2, 200),('2021-07-13 18:50:01', 10004, 1, 100);

select 'select unique table';
select * from delete_from_unique_table order by event_time, product_id, amount;

select '';
DELETE FROM delete_from_unique_table WHERE revenue = 300;
select 'delete data whose revenue is 300';
select 'select unique table';
select * from delete_from_unique_table order by event_time, product_id, amount;

select '';
DELETE FROM delete_from_unique_table WHERE revenue = 100;
select 'delete data whose revenue is 100';
select 'select unique table';
select * from delete_from_unique_table order by event_time, product_id, amount;

select '';
DELETE FROM delete_from_unique_table WHERE revenue >= 500;
select 'delete data whose revenue is bigger than 500';
select 'select unique table';
select * from delete_from_unique_table order by event_time, product_id, amount;

DROP TABLE delete_from_unique_table;

---------------------------------------------
select '';
select '--- A table without partition key columns ---';
drop table if exists unique_table_without_partitionby;

CREATE table unique_table_without_partitionby(
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree()
partition by tuple()
order by (product_id)
unique key product_id;

insert into unique_table_without_partitionby values (10001, 5, 500),(10002, 2, 200),(10003, 1, 100);
insert into unique_table_without_partitionby values (10002, 4, 400),(10003, 2, 200),(10004, 1, 100);

select 'select unique table';
select * from unique_table_without_partitionby order by product_id, amount;

select '';
DELETE FROM unique_table_without_partitionby WHERE revenue = 100;
select 'delete data whose revenue is 100';
select 'select unique table';
select * from unique_table_without_partitionby order by product_id, amount;

---------------------------------------------
select '';
select 'execute DELETE FROM with customized settings';

-- expect a TIMEOUT_EXCEEDED exception
DELETE FROM unique_table_without_partitionby WHERE revenue = 400 AND sleepEachRow(3) SETTINGS max_execution_time = 1; -- { serverError 159}

select '';
select 'select unique table';
select * from unique_table_without_partitionby order by product_id, amount;

DROP TABLE unique_table_without_partitionby;

--------------------------------------------
select '';
select 'execute DELETE FROM with delete bitmap depth issue';
CREATE table unique_table_without_partitionby(
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = CnchMergeTree()
partition by tuple()
order by (product_id)
unique key product_id settings max_delete_bitmap_meta_depth = 1;

insert into unique_table_without_partitionby select number, 100, 500 from system.numbers limit 30;
select count() from unique_table_without_partitionby;
DELETE FROM unique_table_without_partitionby where product_id < 50;
select * from unique_table_without_partitionby;

DROP TABLE unique_table_without_partitionby;
