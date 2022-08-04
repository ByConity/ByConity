set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.drop_partition_where_r1;
drop table if exists test.drop_partition_where_r2;

CREATE table test.drop_partition_where_r1 (
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/drop_partition_where_2', 'r1')
partition by toDate(event_time)
order by (city, category)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

CREATE table test.drop_partition_where_r2 (
    `event_time` DateTime,
    `product_id` UInt64,
    `city` String,
    `category` String,
    `amount` UInt32,
    `revenue` UInt64)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/drop_partition_where_2', 'r2')
partition by toDate(event_time)
ORDER by (city, category)
unique key product_id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

insert into test.drop_partition_where_r1 values ('2020-10-29 23:40:00', 10001, 'Beijing', 'cloth', 5, 500), ('2020-10-29 23:40:00', 10002, 'Beijing', 'cloth', 2, 200), ('2020-10-29 23:40:00', 10003, 'Beijing', 'cloth', 1, 100);
insert into test.drop_partition_where_r1 values ('2020-10-29 23:50:00', 10002, 'Beijing', 'cloth', 4, 400), ('2020-10-29 23:50:00', 10003, 'Beijing', 'cloth', 2, 200), ('2020-10-29 23:50:00', 10004, 'Beijing', 'cloth', 1, 100), ('2020-10-30 00:00:05', 10001, 'Beijing', 'cloth', 1, 100), ('2020-10-30 00:00:05', 10002, 'Beijing', 'cloth', 2, 200);

alter table test.drop_partition_where_r1 drop partition where `toDate(event_time)` = '2020-10-30';

select 'r1', event_time, product_id, city, category, amount, revenue from test.drop_partition_where_r1 order by product_id;
system sync replica test.drop_partition_where_r2;
select 'r2', event_time, product_id, city, category, amount, revenue from test.drop_partition_where_r2 order by product_id;

-- Drop partition from a non-leader replica --
alter table test.drop_partition_where_r2 drop partition where `toDate(event_time)` = '2020-10-29';

system sync replica test.drop_partition_where_r2;
select 'r1', event_time, product_id, city, category, amount, revenue from test.drop_partition_where_r1 order by product_id;
select 'r2', event_time, product_id, city, category, amount, revenue from test.drop_partition_where_r2 order by product_id;

drop table if exists test.drop_partition_where_r1;
drop table if exists test.drop_partition_where_r2;