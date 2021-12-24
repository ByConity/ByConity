set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_with_is_offline_r1;
drop table if exists test.unique_with_is_offline_r2;

create table test.unique_with_is_offline_r1 (d Date, id Int32, s String, offline UInt8) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_is_offline', 'r1', d) partition by d order by (s, id) unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, unique_is_offline_column='offline';
create table test.unique_with_is_offline_r2 (d Date, id Int32, s String, offline UInt8) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_is_offline', 'r2', d) partition by d order by (s, id) unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, unique_is_offline_column='offline';

insert into test.unique_with_is_offline_r1 values ('2020-10-25', 10, '10A', 0), ('2020-10-25', 11, '11A', 0), ('2020-10-26', 10, '10A', 0), ('2020-10-26', 11, '11A', 0), ('2020-10-26', 10, '10B', 0);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

insert into test.unique_with_is_offline_r1 values ('2020-10-27', 10, '10A', 0), ('2020-10-26', 10, '10A-off', 1), ('2020-10-26', 11, '11A-off', 1), ('2020-10-27', 10, '10B', 0);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

insert into test.unique_with_is_offline_r1 values ('2020-10-25', 10, '10A', 0), ('2020-10-26', 10, '10A', 0), ('2020-10-27', 11, '11A', 0);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

insert into test.unique_with_is_offline_r1 values ('2020-10-25', 10, '10A-off', 1);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

insert into test.unique_with_is_offline_r1 values ('2020-10-26', 10, '10B-off', 1);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

insert into test.unique_with_is_offline_r1 values ('2020-10-27', 10, '10A-off', 1);
select 'r1', d, id, s, offline from test.unique_with_is_offline_r1 order by d, id;

select sleep(3) format Null;
select 'r2', d, id, s, offline from test.unique_with_is_offline_r2 order by d, id;

drop table if exists test.unique_with_is_offline_r1;
drop table if exists test.unique_with_is_offline_r2;

drop table if exists test.unique_with_is_offline_bad1;
drop table if exists test.unique_with_is_offline_bad2;
drop table if exists test.unique_with_is_offline_bad3;

create table test.unique_with_is_offline_bad1 (d Date, id Int32, s String, offline String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_is_offline_bad1', 'r1', d) partition by d order by (s, id) unique key id SETTINGS unique_is_offline_column='offline'; -- { serverError 169 }
create table test.unique_with_is_offline_bad2 (d Date, id Int32, s String) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_is_offline_bad2', 'r1', d) partition by d order by (s, id) unique key id SETTINGS unique_is_offline_column='offline'; -- { serverError 16 }
create table test.unique_with_is_offline_bad3 (d Date, id Int32, s String, offline UInt8) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_with_is_offline_bad3', 'r1') partition by d order by (s, id) unique key id SETTINGS unique_is_offline_column='offline'; -- { serverError 36 }
