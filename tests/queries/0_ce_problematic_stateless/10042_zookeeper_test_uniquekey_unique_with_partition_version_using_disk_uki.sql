set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

-- Test in memory unique index, set `enable_disk_based_unique_key_index=0` to disable disked based
drop table if exists test.unique_with_partition_version_uki_r1;
drop table if exists test.unique_with_partition_version_uki_r2;

create table test.unique_with_partition_version_uki_r1 (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_partition_version_test_uki', 'r1', toDate(event_time)) partition by toDate(event_time) order by s unique key id
SETTINGS partition_level_unique_keys=0, ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=0;

create table test.unique_with_partition_version_uki_r2 (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_partition_version_test_uki', 'r2', toDate(event_time)) partition by toDate(event_time) order by s unique key id
SETTINGS partition_level_unique_keys=0, ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=0, replicated_can_become_leader=0;

insert into test.unique_with_partition_version_uki_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);
insert into test.unique_with_partition_version_uki_r1 values ('2020-10-30 00:05:00', 10002, '10002C', 10, 1000), ('2020-10-30 00:05:00', 10003, '10003A', 3, 300);
system sync replica test.unique_with_partition_version_uki_r2;
select 'r1', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r2 order by event_time, id;

insert into test.unique_with_partition_version_uki_r2 values ('2020-10-29 23:50:00', 10001, '10001B', 7, 700), ('2020-10-29 23:50:00', 10002, '10002B', 6, 600);
system sync replica test.unique_with_partition_version_uki_r2;
select 'r1', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r2 order by event_time, id;

insert into test.unique_with_partition_version_uki_r2 values ('2020-10-29 23:59:59', 10004, '10004A', 1, 100), ('2020-10-29 23:59:59', 10004, '10004B', 2, 200), ('2020-10-30 00:00:00', 10004, '10004C', 3, 300), ('2020-10-30 00:00:00', 10004, '10004D', 4, 400);
system sync replica test.unique_with_partition_version_uki_r2;
select 'r1', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from test.unique_with_partition_version_uki_r2 order by event_time, id;

drop table if exists test.unique_with_partition_version_uki_r1;
drop table if exists test.unique_with_partition_version_uki_r2;
