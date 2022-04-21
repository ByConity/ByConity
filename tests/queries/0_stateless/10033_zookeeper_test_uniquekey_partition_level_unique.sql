set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.partition_level_unique_r1;
drop table if exists test.partition_level_unique_r2;

create table test.partition_level_unique_r1 (d Date, id Int32, s String, arr Array(Int32), sum materialized arraySum(arr))
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_test', 'r1') partition by d order by (s, id) primary key s unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

create table test.partition_level_unique_r2 (d Date, id Int32, s String, arr Array(Int32), sum materialized arraySum(arr))
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_test', 'r2') partition by d order by (s, id) primary key s unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

insert into test.partition_level_unique_r1 values ('2020-10-29', 1001, '1001A', [1,2]), ('2020-10-29', 1002, '1002A', [3,4]), ('2020-10-29', 1001, '1001B', [5,6]), ('2020-10-29', 1001, '1001C', [7,8]);

system sync replica test.partition_level_unique_r2;
select 'r1', d, id, s, arr, sum from test.partition_level_unique_r1 order by d, id;
select 'r2', d, id, s, arr, sum from test.partition_level_unique_r2 order by d, id;

insert into test.partition_level_unique_r2 values ('2020-10-29', 1002, '1002B', [9, 10]), ('2020-10-30', 1001, '1001A', [1,2]), ('2020-10-30', 1002, '1002A', [3,4]), ('2020-10-30', 1001, '1001B', [5,6]);

system sync replica test.partition_level_unique_r2;
select 'r1', d, id, s, arr, sum from test.partition_level_unique_r1 order by d, id;
select 'r2', d, id, s, arr, sum from test.partition_level_unique_r2 order by d, id;

-- Test select total row
system sync replica test.partition_level_unique_r2;
select count(*) from test.partition_level_unique_r1;

-- Test select rows in partition
select count(*) from test.partition_level_unique_r1 where d=toDate('2020-10-29');

drop table if exists test.partition_level_unique_r1;
drop table if exists test.partition_level_unique_r2;
