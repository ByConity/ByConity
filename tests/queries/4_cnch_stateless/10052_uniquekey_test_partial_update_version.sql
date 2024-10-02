drop table if exists unique_with_version_r1;
drop table if exists unique_with_version_r2;

select '----------- test1, set partial_update_enable_merge_map to true -----------';

create table unique_with_version_r1 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64,
    mp Map(String, Int32))
Engine=CnchMergeTree(event_time) partition by toDate(event_time) order by id unique key id
SETTINGS enable_unique_partial_update = 1;

set enable_unique_partial_update = 1;

insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, {'a': 1}), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200, {'b': 2}), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800, {'c': 3}), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500, {'d': 4});

insert into unique_with_version_r1 values ('2020-10-30 00:05:00', 10001, '10001A', 1, 100, {'e': 5}), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200, {'f': 6});
insert into unique_with_version_r1 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, {'g': 7}), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200, {'h': 8});

select 'insert with partial update mode';

select 'r1', event_time, id, s, m1, m2, mp from unique_with_version_r1 order by event_time, id;

insert into unique_with_version_r1 (event_time, id, s, m1) values ('2020-10-29 23:50:00', 10001, '10001B', 8), ('2020-10-29 23:50:00', 10002, '10002B', 5), ('2020-10-29 23:55:00', 10001, '10001C', 10), ('2020-10-29 23:55:00', 10002, '10002C', 7);

select 'r1', event_time, id, s, m1, m2, mp from unique_with_version_r1 order by event_time, id;

drop table if exists unique_with_version_r1;

select '----------- test2, set partial_update_enable_merge_map to false -----------';

create table unique_with_version_r2 (
    event_time DateTime,
    id UInt64,
    s String,
    m1 UInt32,
    m2 UInt64,
    mp Map(String, Int32))
Engine=CnchMergeTree(event_time) partition by toDate(event_time) order by id unique key id
SETTINGS enable_unique_partial_update = 1, partial_update_enable_merge_map = 0;

set enable_unique_partial_update = 1;

insert into unique_with_version_r2 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, {'a': 1}), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200, {'b': 2}), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800, {'c': 3}), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500, {'d': 4});

insert into unique_with_version_r2 values ('2020-10-30 00:05:00', 10001, '10001A', 1, 100, {'e': 5}), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200, {'f': 6});
insert into unique_with_version_r2 values ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, {'g': 7}), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200, {'h': 8});

select 'insert with partial update mode';

select 'r2', event_time, id, s, m1, m2, mp from unique_with_version_r2 order by event_time, id;

insert into unique_with_version_r2 (event_time, id, s, m1) values ('2020-10-29 23:50:00', 10001, '10001B', 8), ('2020-10-29 23:50:00', 10002, '10002B', 5), ('2020-10-29 23:55:00', 10001, '10001C', 10), ('2020-10-29 23:55:00', 10002, '10002C', 7);

select 'r2', event_time, id, s, m1, m2, mp from unique_with_version_r2 order by event_time, id;

drop table if exists unique_with_version_r2;
