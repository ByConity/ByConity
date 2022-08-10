set database_atomic_wait_for_drop_and_detach_synchronously = 1;
drop table if exists test.unique_create_table_as_r1;
drop table if exists test.unique_create_table_as_r2;
drop table if exists test.unique_create_table_as_tmp;

CREATE table test.unique_create_table_as_r1 (
    `event_time` Date,
    `product_id` UInt64,
    `is_off` UInt8,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_create_table_as', 'r1', event_time)
partition by event_time
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, unique_is_offline_column = 'is_off';

CREATE table test.unique_create_table_as_r2 (
    `event_time` Date,
    `product_id` UInt64,
    `is_off` UInt8,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_create_table_as', 'r2', event_time)
partition by event_time
unique key product_id
order by product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, unique_is_offline_column = 'is_off', replicated_can_become_leader=0;


-- Create a temp table for r1
create table test.unique_create_table_as_tmp as test.unique_create_table_as_r1 IGNORE REPLICATED;

-- Insert immediately after creating the temp table
insert into test.unique_create_table_as_tmp values ('2020-10-29', 10001, 0, 1);

system sync replica test.unique_create_table_as_r2;

select 't1', event_time, product_id, is_off, val from test.unique_create_table_as_tmp order by product_id;

drop table if exists test.unique_create_table_as_r1;
drop table if exists test.unique_create_table_as_r2;
drop table if exists test.unique_create_table_as_tmp;
