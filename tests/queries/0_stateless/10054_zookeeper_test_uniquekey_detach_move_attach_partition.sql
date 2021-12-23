set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_detach_move_attach_partition_r1;
drop table if exists test.unique_detach_move_attach_partition_r2;

drop table if exists test.unique_detach_move_attach_partition_r3;
drop table if exists test.unique_detach_move_attach_partition_r4;

CREATE table test.unique_detach_move_attach_partition_r1 (
    `event_time` DateTime,
    `product_id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/unique_test_one', 'r1')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.unique_detach_move_attach_partition_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/unique_test_one', 'r2')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.unique_detach_move_attach_partition_r3 (
    `event_time` DateTime,
    `product_id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/unique_test_two', 'r3')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.unique_detach_move_attach_partition_r4 (
    `event_time` DateTime,
    `product_id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/unique_test_two', 'r4')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

-- Insert data into part1
insert into test.unique_detach_move_attach_partition_r1 values ('2020-10-29', 10001, 1);

-- Insert data into part2, same partition, same unique key, it should "delete" the row the part1
insert into test.unique_detach_move_attach_partition_r1 values ('2020-10-29', 10001, 2);

select sleep(3) format Null;
select 'r1', event_time, product_id, val from test.unique_detach_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id, val from test.unique_detach_move_attach_partition_r2 order by product_id;
select 'r3', event_time, product_id, val from test.unique_detach_move_attach_partition_r3 order by product_id;
select 'r4', event_time, product_id, val from test.unique_detach_move_attach_partition_r4 order by product_id;

-- Drop data from r1
alter table test.unique_detach_move_attach_partition_r1 detach partition '2020-10-29';

select sleep(3) format Null;
select 'r1', event_time, product_id, val from test.unique_detach_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id, val from test.unique_detach_move_attach_partition_r2 order by product_id;

-- Move droped partition from r1 to r3
alter table test.unique_detach_move_attach_partition_r3 move partition '2020-10-29' from test.unique_detach_move_attach_partition_r1;

select sleep(3) format Null;

-- Attach partition into r3
-- After attaching this paritition back, thr row also needs to be "deleted"
alter table test.unique_detach_move_attach_partition_r3 attach partition '2020-10-29';

select sleep(3) format Null;
select 'r1', event_time, product_id, val from test.unique_detach_move_attach_partition_r1 order by product_id;
select 'r2', event_time, product_id, val from test.unique_detach_move_attach_partition_r2 order by product_id;
select 'r3', event_time, product_id, val from test.unique_detach_move_attach_partition_r3 order by product_id;
select 'r4', event_time, product_id, val from test.unique_detach_move_attach_partition_r4 order by product_id;

drop table if exists test.unique_detach_move_attach_partition_r1;
drop table if exists test.unique_detach_move_attach_partition_r2;
drop table if exists test.unique_detach_move_attach_partition_r3;
drop table if exists test.unique_detach_move_attach_partition_r4;
