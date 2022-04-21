set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.delete_source_table;
drop table if exists test.delete_by_unique_key_with_view_r1;
drop table if exists test.delete_by_unique_key_with_view_r2;
drop table if exists test.delete_by_unique_key_with_view_view;

CREATE table test.delete_source_table(
     `event_time` DateTime,
     `product_id` UInt64,
     `amount` UInt32,
     `revenue` UInt64,
     `_delete_flag_` UInt8)
ENGINE = MergeTree
partition by toDate(event_time)
order by (event_time, product_id);

CREATE table test.delete_by_unique_key_with_view_r1(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/delete_by_unique_key_with_view', 'r1')
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

CREATE table test.delete_by_unique_key_with_view_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/delete_by_unique_key_with_view', 'r2')
partition by toDate(event_time)
ORDER by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

CREATE MATERIALIZED VIEW test.delete_by_unique_key_with_view_view TO test.delete_by_unique_key_with_view_r1 (`event_time` DateTime, `product_id` UInt64, `amount` UInt32, `revenue` UInt64, `_delete_flag_` UInt8) AS SELECT * FROM test.delete_source_table;

select 'create materialized view from source table to ha unique table';

insert into test.delete_source_table values ('2021-07-13 18:50:00', 10001, 5, 500, 0),('2021-07-13 18:50:00', 10002, 2, 200, 0),('2021-07-13 18:50:00', 10003, 1, 100, 0);
insert into test.delete_source_table values ('2021-07-13 18:50:01', 10002, 4, 400, 0),('2021-07-14 18:50:01', 10003, 2, 200, 0),('2021-07-13 18:50:01', 10004, 1, 100, 0);

select 'select source table';
select * from test.delete_source_table order by event_time, product_id, amount, _delete_flag_;
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_view_r1 order by event_time, product_id, amount;
system sync replica test.delete_by_unique_key_with_view_r2;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_view_r2 order by event_time, product_id, amount;

select '';
insert into test.delete_source_table values ('2021-07-13 18:50:01', 10002, 5, 500, 1),('2021-07-14 18:50:00', 10003, 2, 200, 1);
select 'delete data of pair(2021-07-13, 10002) and pair(2021-07-14, 10003)';
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_view_r1 order by event_time, product_id, amount;
system sync replica test.delete_by_unique_key_with_view_r2;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_view_r2 order by event_time, product_id, amount;

select '';
insert into test.delete_source_table select event_time, product_id, amount, revenue, 1 as _delete_flag_ from test.delete_source_table where revenue >= 500;
select 'delete data whose revenue is bigger than 500';
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_view_r1 order by event_time, product_id, amount;
system sync replica test.delete_by_unique_key_with_view_r2;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_view_r2 order by event_time, product_id, amount;

select '';
drop table if exists test.delete_source_table;
drop table if exists test.delete_by_unique_key_with_view_view;
CREATE table test.delete_source_table(
     `event_time` DateTime,
     `product_id` UInt64,
     `amount` UInt32,
     `revenue` UInt64,
     `_delete_flag_` UInt8)
ENGINE = MergeTree
partition by toDate(event_time)
order by (event_time, product_id);

CREATE MATERIALIZED VIEW test.delete_by_unique_key_with_view_view TO test.delete_by_unique_key_with_view_r2 (`event_time` DateTime, `product_id` UInt64, `amount` UInt32, `revenue` UInt64, `_delete_flag_` UInt8) AS SELECT event_time, product_id, amount, revenue, amount < 10 as _delete_flag_ FROM test.delete_source_table;
select 'clear source table and update materialized view to another replica, it will add new rows whose amount >= 10, delete old rows whose amount < 10, using expressions';
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_view_r1 order by event_time, product_id, amount;
system sync replica test.delete_by_unique_key_with_view_r2;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_view_r2 order by event_time, product_id, amount;

select '';
insert into test.delete_source_table values ('2021-07-13 18:50:01', 10003, 4, 400, 0),('2021-07-16 18:50:01', 10003, 20, 200, 0),('2021-07-16 18:50:01', 10004, 9, 100, 0);
select 'insert 3 rows, the amount of 2 rows is less than 10, which will delete pair (2021-07-13, 10003) and add pair (2021-07-16, 10003)';
select 'select source table';
select * from test.delete_source_table order by event_time, product_id, amount, _delete_flag_;
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_view_r1 order by event_time, product_id, amount;
system sync replica test.delete_by_unique_key_with_view_r2;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_view_r2 order by event_time, product_id, amount;

drop table if exists test.delete_source_table;
drop table if exists test.delete_by_unique_key_with_view_r1;
drop table if exists test.delete_by_unique_key_with_view_r2;
drop table if exists test.delete_by_unique_key_with_view_view;
