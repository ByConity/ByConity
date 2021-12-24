set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.delete_by_unique_key_with_version_r1;
drop table if exists test.delete_by_unique_key_with_version_r2;

CREATE table test.delete_by_unique_key_with_version_r1(
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64,
    `version` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/delete_by_unique_key_with_version', 'r1', version)
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

CREATE table test.delete_by_unique_key_with_version_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `amount` UInt32,
    `revenue` UInt64,
    `version` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/delete_by_unique_key_with_version', 'r2', version)
partition by toDate(event_time)
order by (event_time, product_id)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.delete_by_unique_key_with_version_r1 values ('2021-07-13 18:50:00', 10001, 5, 500, 2),('2021-07-13 18:50:00', 10002, 2, 200, 2),('2021-07-13 18:50:00', 10003, 1, 100, 2);
insert into test.delete_by_unique_key_with_version_r1 values ('2021-07-13 18:50:01', 10002, 4, 400, 1),('2021-07-14 18:50:01', 10003, 2, 200, 2),('2021-07-13 18:50:01', 10004, 1, 100, 2);

select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_version_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_version_r2 order by event_time, product_id, amount;

select '';
insert into test.delete_by_unique_key_with_version_r1 (event_time, product_id, amount, revenue, version, _delete_flag_) values ('2021-07-13 18:50:01', 10002, 5, 500, 1, 1),('2021-07-14 18:50:00', 10003, 2, 200, 1, 1);
select 'delete data with lower version, which will not take effect';
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_version_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_version_r2 order by event_time, product_id, amount;

select '';
insert into test.delete_by_unique_key_with_version_r1 (event_time, product_id, amount, revenue, version, _delete_flag_) values ('2021-07-13 18:50:01', 10002, 5, 500, 5, 1),('2021-07-14 18:50:00', 10003, 2, 200, 5, 1);
select 'delete data with higher version, which will take effect, delete pair(2021-07-13, 10002) and pair(2021-07-14, 10003)';
select 'select ha unique table r1';
select * from test.delete_by_unique_key_with_version_r1 order by event_time, product_id, amount;
select sleep(3) format Null;
select 'select ha unique table r2';
select * from test.delete_by_unique_key_with_version_r2 order by event_time, product_id, amount;

-- FIXME (UNIQUE KEY): uncomment these after done: src/Storages/MergeTree/MergeTreeRangeReader.cpp:1034
-- select '';
-- insert into test.delete_by_unique_key_with_version_r2 (event_time, product_id, amount, revenue, _delete_flag_) select event_time, product_id, amount, revenue, 1 as _delete_flag_ from test.delete_by_unique_key_with_version_r1 where revenue >= 500;
-- select 'delete data with ignoring version whose revenue is bigger than 500 using insert select, write to another replica';
-- select 'select ha unique table r1';
-- select * from test.delete_by_unique_key_with_version_r1 order by event_time, product_id, amount;
-- select sleep(3) format Null;
-- select 'select ha unique table r2';
-- select * from test.delete_by_unique_key_with_version_r2 order by event_time, product_id, amount;

drop table if exists test.delete_by_unique_key_with_version_r1;
drop table if exists test.delete_by_unique_key_with_version_r2;
