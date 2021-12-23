set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.truncate_r1;
drop table if exists test.truncate_r2;

CREATE table test.truncate_r1 (
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/truncate_test_1', 'r1')
partition by toDate(event_time)
order by (city, category)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

CREATE table test.truncate_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/truncate_test_1', 'r2')
partition by toDate(event_time)
ORDER by (city, category)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.truncate_r1 values ('2020-10-29 23:40:00', 10001, 'Beijing', 'cloth', 1, 100);

select 'r1', event_time, product_id, city, category, amount, revenue from test.truncate_r1 order by product_id;
select sleep(3) format Null;
select 'r2', event_time, product_id, city, category, amount, revenue from test.truncate_r2 order by product_id;

-- Truncate table from a non-leader replica --
truncate table test.truncate_r2;

select sleep(3) format Null;
select 'r1', event_time, product_id, city, category, amount, revenue from test.truncate_r1 order by product_id;
select 'r2', event_time, product_id, city, category, amount, revenue from test.truncate_r2 order by product_id;

drop table if exists test.truncate_r1;
drop table if exists test.truncate_r2;