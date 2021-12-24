set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_disk_based_unique_key_index_method = 1;

drop table if exists test.unique_with_version_uki_bad1;
drop table if exists test.unique_with_version_uki_bad2;
create table test.unique_with_version_uki_bad1 (event_time DateTime, id UInt64, s String, m1 UInt32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_version_uki_bad1', 'r1', m2) partition by toDate(event_time) order by s unique key id; -- { serverError 16 }
create table test.unique_with_version_uki_bad2 (event_time DateTime, id UInt64, s String, m1 UInt32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_version_uki_bad2', 'r1', sipHash64(s)) partition by toDate(event_time) order by s unique key id; -- { serverError 36 }
drop table if exists test.unique_with_version_uki_bad1;
drop table if exists test.unique_with_version_uki_bad2;

drop table if exists test.unique_with_version_uki_r1;
drop table if exists test.unique_with_version_uki_r2;

create table test.unique_with_version_uki_r1 (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_version_uki_1', 'r1', event_time) partition by toDate(event_time) order by s unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=1;
create table test.unique_with_version_uki_r2 (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_version_uki_1', 'r2', event_time) partition by toDate(event_time) order by s unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_disk_based_unique_key_index=1;

insert into test.unique_with_version_uki_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);
insert into test.unique_with_version_uki_r1 values ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
insert into test.unique_with_version_uki_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

select sleep(3) format Null;
select 'r1', event_time, id, s, m1, m2 from test.unique_with_version_uki_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from test.unique_with_version_uki_r2 order by event_time, id;

insert into test.unique_with_version_uki_r2 values ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500), ('2020-10-29 23:55:00', 10001, '10001C', 10, 1000), ('2020-10-29 23:55:00', 10002, '10002C', 7, 700);

select sleep(3) format Null;
select 'r1', event_time, id, s, m1, m2 from test.unique_with_version_uki_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from test.unique_with_version_uki_r2 order by event_time, id;

drop table if exists test.unique_with_version_uki_r1;
drop table if exists test.unique_with_version_uki_r2;
