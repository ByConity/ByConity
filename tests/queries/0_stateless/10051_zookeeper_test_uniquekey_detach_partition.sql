set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_detach_partition_r1;
drop table if exists test.unique_detach_partition_r2;

CREATE table test.unique_detach_partition_r1 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/detach_partition_test', 'r1')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.unique_detach_partition_r2 (
    `event_time` DateTime,
    `product_id` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/detach_partition_test', 'r2')
partition by toDate(event_time)
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.unique_detach_partition_r1 values ('2020-10-29', 10001), ('2020-10-30', 10002), ('2020-10-31', 10003);

select sleep(3) format Null;
select 'r1', event_time, product_id from test.unique_detach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.unique_detach_partition_r2 order by product_id;

alter table test.unique_detach_partition_r1 detach partition '2020-10-29';

select sleep(3) format Null;
select 'r1', event_time, product_id from test.unique_detach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.unique_detach_partition_r2 order by product_id;

alter table test.unique_detach_partition_r1 detach partition '2020-10-30';

select sleep(3) format Null;
select 'r1', event_time, product_id from test.unique_detach_partition_r1 order by product_id;
select 'r2', event_time, product_id from test.unique_detach_partition_r2 order by product_id;

drop table if exists test.unique_detach_partition_r1;
drop table if exists test.unique_detach_partition_r2;
