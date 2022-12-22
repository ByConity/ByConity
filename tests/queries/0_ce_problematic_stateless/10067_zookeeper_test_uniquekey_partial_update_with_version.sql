set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_unique_partial_update = 1;

drop table if exists unique_with_version_r1;
drop table if exists unique_with_version_r2;

-- test partial update mode, disable unique row store
create table unique_with_version_r1 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/10067_zookeeper_test_uniquekey_partial_update_with_version/unique_with_version_1', 'r1', event_time) partition by toDate(event_time) order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

create table unique_with_version_r2 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/10067_zookeeper_test_uniquekey_partial_update_with_version/unique_with_version_1', 'r2', event_time) partition by toDate(event_time) order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0, replicated_can_become_leader=0;

insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);

insert into unique_with_version_r1 values ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

select 'insert with partial update mode, disable unique row store';
system sync replica unique_with_version_r2;
select 'r1', event_time, id, s, m1, m2 from unique_with_version_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from unique_with_version_r2 order by event_time, id;

optimize table unique_with_version_r1 final;
insert into unique_with_version_r2 (event_time, id, s, m1) values ('2020-10-29 23:50:00', 10001, '10001B', 8), ('2020-10-29 23:50:00', 10002, '10002B', 5), ('2020-10-29 23:55:00', 10001, '10001C', 10), ('2020-10-29 23:55:00', 10002, '10002C', 7);
select 'after merge, update two rows in partial update mode';
system sync replica unique_with_version_r2;
select 'r1', event_time, id, s, m1, m2 from unique_with_version_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from unique_with_version_r2 order by event_time, id;

drop table if exists unique_with_version_r1;
drop table if exists unique_with_version_r2;

select '----------------------------';
-- test partial update mode, enable unique row store
create table unique_with_version_r1 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/10067_zookeeper_test_uniquekey_partial_update_with_version/unique_with_version_1', 'r1', event_time) partition by toDate(event_time) order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

create table unique_with_version_r2 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/10067_zookeeper_test_uniquekey_partial_update_with_version/unique_with_version_1', 'r2', event_time) partition by toDate(event_time) order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1, replicated_can_become_leader=0;

insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);

insert into unique_with_version_r1 values ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

select 'insert with partial update mode, enable unique row store';
system sync replica unique_with_version_r2;
select 'r1', event_time, id, s, m1, m2 from unique_with_version_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from unique_with_version_r2 order by event_time, id;

optimize table unique_with_version_r2 final;
insert into unique_with_version_r2 (event_time, id, s, m1) values ('2020-10-29 23:50:00', 10001, '10001B', 8), ('2020-10-29 23:50:00', 10002, '10002B', 5), ('2020-10-29 23:55:00', 10001, '10001C', 10), ('2020-10-29 23:55:00', 10002, '10002C', 7);
select 'after merge, update two rows in partial update mode';
system sync replica unique_with_version_r2;
select 'r1', event_time, id, s, m1, m2 from unique_with_version_r1 order by event_time, id;
select 'r2', event_time, id, s, m1, m2 from unique_with_version_r2 order by event_time, id;

drop table if exists unique_with_version_r1;
drop table if exists unique_with_version_r2;
