set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.drop_move_attach_partition_r1;
drop table if exists test.drop_move_attach_partition_r2;

drop table if exists test.drop_move_attach_partition_r3;
drop table if exists test.drop_move_attach_partition_r4;

CREATE table test.drop_move_attach_partition_r1 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/move_partition_test_one', 'r1')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.drop_move_attach_partition_r2 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/move_partition_test_one', 'r2')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.drop_move_attach_partition_r3 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/move_partition_test_two', 'r3')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.drop_move_attach_partition_r4 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/move_partition_test_two', 'r4')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

-- Insert data into r1 and r2
insert into test.drop_move_attach_partition_r1 values ('2020-10-29', 10001), ('2020-10-30', 10002), ('2020-10-31', 10003);

select sleep(3) format Null;
select 'r1', event_time, product_id from test.drop_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.drop_move_attach_partition_r2 order by product_id;
select 'r3', event_time, product_id from test.drop_move_attach_partition_r3 order by product_id;
select 'r4', event_time, product_id from test.drop_move_attach_partition_r4 order by product_id;

-- Drop data from r1
alter table test.drop_move_attach_partition_r1 detach partition '2020-10-29';

select sleep(3) format Null;
select 'r1', event_time, product_id from test.drop_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.drop_move_attach_partition_r2 order by product_id;

-- Move droped partition from r1 to r3
alter table test.drop_move_attach_partition_r3 move partition '2020-10-29' from test.drop_move_attach_partition_r1;

select sleep(3) format Null;

-- Attach partition into r3
alter table test.drop_move_attach_partition_r3 attach partition '2020-10-29';

select sleep(3) format Null;
select 'r1', event_time, product_id from test.drop_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.drop_move_attach_partition_r2 order by product_id;
select 'r3', event_time, product_id from test.drop_move_attach_partition_r3 order by product_id;
select 'r4', event_time, product_id from test.drop_move_attach_partition_r4 order by product_id;

drop table if exists test.drop_move_attach_partition_r1;
drop table if exists test.drop_move_attach_partition_r2;
drop table if exists test.drop_move_attach_partition_r3;
drop table if exists test.drop_move_attach_partition_r4;
