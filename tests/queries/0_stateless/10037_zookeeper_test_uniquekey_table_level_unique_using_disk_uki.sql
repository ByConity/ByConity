set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_disk_based_unique_key_index_method = 1;

drop table if exists test.table_level_unique_uki_r1;
drop table if exists test.table_level_unique_uki_r2;

create table test.table_level_unique_uki_r1 (d Date, id Int32, s String, arr Array(Int32), sum materialized arraySum(arr)) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/table_level_unique_uki', 'r1') partition by d order by (s, id) primary key s unique key id SETTINGS partition_level_unique_keys=0, ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=1;
create table test.table_level_unique_uki_r2 (d Date, id Int32, s String, arr Array(Int32), sum materialized arraySum(arr)) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/table_level_unique_uki', 'r2') partition by d order by (s, id) primary key s unique key id SETTINGS partition_level_unique_keys=0, ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=1;

insert into test.table_level_unique_uki_r1 values ('2020-10-25', 1001, '1001A', [1,1]), ('2020-10-25', 1002, '1002A', [2,1]), ('2020-10-25', 1001, '1001B', [1,2]), ('2020-10-26', 1002, '1002B', [2,2]), ('2020-10-26', 1003, '1003A', [3,1]);
insert into test.table_level_unique_uki_r1 values ('2020-10-27', 1003, '1003B', [3,2]), ('2020-10-27', 1004, '1004A', [4,1]);

select sleep(3) format Null;
select 'r1', d, id, s, arr, sum from test.table_level_unique_uki_r1 order by d, id;
select 'r2', d, id, s, arr, sum from test.table_level_unique_uki_r2 order by d, id;

insert into test.table_level_unique_uki_r2 values ('2020-10-25', 1001, '1001A', [1, 1]), ('2020-10-25', 1002, '1002A', [2,1]), ('2020-10-25', 1003, '1003A', [3,1]), ('2020-10-25', 1004, '1004A', [4,1]);

select sleep(3) format Null;
select 'r1', d, id, s, arr, sum from test.table_level_unique_uki_r1 order by d, id;
select 'r2', d, id, s, arr, sum from test.table_level_unique_uki_r2 order by d, id;

drop table if exists test.table_level_unique_uki_r1;
drop table if exists test.table_level_unique_uki_r2;
