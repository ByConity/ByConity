set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_unique_partial_update = 1;

drop table if exists delete_by_unique_key_r1;
drop table if exists delete_by_unique_key_r2;

-- test partial update mode, disable unique row store
CREATE table delete_by_unique_key_r1(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10068_zookeeper_test_uniquekey_partial_update_delete_by_unique_key/delete_by_unique_key', 'r1')
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

CREATE table delete_by_unique_key_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10068_zookeeper_test_uniquekey_partial_update_delete_by_unique_key/delete_by_unique_key', 'r2')
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

insert into delete_by_unique_key_r1 values ('2021-07-13 18:50:00', 10001, 5, 500),('2021-07-13 18:50:00', 10002, 2, 200),('2021-07-13 18:50:00', 10003, 1, 100);

insert into delete_by_unique_key_r1 values ('2021-07-13 18:50:01', 10002, 4, 400),('2021-07-14 18:50:01', 10003, 2, 200),('2021-07-13 18:50:01', 10004, 1, 100);

select 'insert with partial update mode, disable unique row store';
select 'select ha unique table r1';
select * from delete_by_unique_key_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from delete_by_unique_key_r2 order by event_time, product_id, amount;

select '';
insert into delete_by_unique_key_r1 (event_time, product_id, amount, _delete_flag_) 
values ('2021-07-13 18:50:01', 10002, 5, 1),('2021-07-14 18:50:00', 10003, 2, 1),('2021-07-13 18:50:00', 10001, 2, 0), ('2021-07-15 18:50:00', 10004, 2, 0)
select 'delete data of pair(2021-07-13, 10002) and pair(2021-07-14, 10003), insert two new rows';
select 'select ha unique table r1';
select * from delete_by_unique_key_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from delete_by_unique_key_r2 order by event_time, product_id, amount;

drop table if exists delete_by_unique_key_r1;
drop table if exists delete_by_unique_key_r2;

select '---------------------------';
-- test partial update mode, enable unique row store
CREATE table delete_by_unique_key_r1(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10068_zookeeper_test_uniquekey_partial_update_delete_by_unique_key/delete_by_unique_key', 'r1')
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

CREATE table delete_by_unique_key_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10068_zookeeper_test_uniquekey_partial_update_delete_by_unique_key/delete_by_unique_key', 'r2')
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

insert into delete_by_unique_key_r1 values ('2021-07-13 18:50:00', 10001, 5, 500),('2021-07-13 18:50:00', 10002, 2, 200),('2021-07-13 18:50:00', 10003, 1, 100);

insert into delete_by_unique_key_r1 values ('2021-07-13 18:50:01', 10002, 4, 400),('2021-07-14 18:50:01', 10003, 2, 200),('2021-07-13 18:50:01', 10004, 1, 100);

select 'insert with partial update mode, enable unique row store';
select 'select ha unique table r1';
select * from delete_by_unique_key_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from delete_by_unique_key_r2 order by event_time, product_id, amount;

select '';
insert into delete_by_unique_key_r1 (event_time, product_id, amount, _delete_flag_) 
values ('2021-07-13 18:50:01', 10002, 5, 1),('2021-07-14 18:50:00', 10003, 2, 1),('2021-07-13 18:50:00', 10001, 2, 0), ('2021-07-15 18:50:00', 10004, 2, 0)
select 'delete data of pair(2021-07-13, 10002) and pair(2021-07-14, 10003), insert two new rows';
select 'select ha unique table r1';
select * from delete_by_unique_key_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from delete_by_unique_key_r2 order by event_time, product_id, amount;

drop table if exists delete_by_unique_key_r1;
drop table if exists delete_by_unique_key_r2;
