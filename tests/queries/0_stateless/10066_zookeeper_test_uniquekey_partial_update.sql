set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_unique_partial_update = 1;

drop table if exists test.uniquekey_partial_update1;
drop table if exists test.uniquekey_partial_update2;

------- test normal insert operation
create table test.uniquekey_partial_update1 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 0, enable_unique_row_store = 0;
create table test.uniquekey_partial_update2 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 0, enable_unique_row_store = 0;

insert into test.uniquekey_partial_update1 values (1, 2, 3), (2, 3, 4);
insert into test.uniquekey_partial_update1 (id, b) values (1, 6), (2, 8);
select sleep(3) format Null;
select 'insert with normal mode, using default value';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

optimize table test.uniquekey_partial_update1 final;
insert into test.uniquekey_partial_update1 (id, a) values (1, 4), (2, 6);
select sleep(3) format Null;
select 'after merge, insert with normal mode';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

drop table if exists test.uniquekey_partial_update1;
drop table if exists test.uniquekey_partial_update2;

select '';
------- test partial update insert operation, disable unique_row_store
create table test.uniquekey_partial_update1 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;
create table test.uniquekey_partial_update2 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

insert into test.uniquekey_partial_update1 values (1, 2, 3), (2, 3, 4);
insert into test.uniquekey_partial_update1 (id, b) values (1, 6), (2, 8);
select sleep(3) format Null;
select 'insert with partial update mode, disable unique row store';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

optimize table test.uniquekey_partial_update1 final;
insert into test.uniquekey_partial_update1 (id, a) values (1, 4), (2, 6);
select sleep(3) format Null;
select 'after merge, insert with partial update mode, disable unique row store';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

drop table if exists test.uniquekey_partial_update1;
drop table if exists test.uniquekey_partial_update2;

select '';
------- test partial update insert operation, enable unique_row_store
create table test.uniquekey_partial_update1 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;
create table test.uniquekey_partial_update2 (id Int32, a Int32, b Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

insert into test.uniquekey_partial_update1 values (1, 2, 3), (2, 3, 4);
insert into test.uniquekey_partial_update1 (id, b) values (1, 6), (2, 8);
select sleep(3) format Null;
select 'insert with partial update mode, enable unique row store';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

optimize table test.uniquekey_partial_update1 final;
insert into test.uniquekey_partial_update1 (id, a) values (1, 4), (2, 6);
select sleep(3) format Null;
select 'after merge, insert with partial update mode, enable unique row store';
select 'select replica 1';
select * from test.uniquekey_partial_update1;
select 'select replica 2';
select * from test.uniquekey_partial_update2;

drop table if exists test.uniquekey_partial_update1;
drop table if exists test.uniquekey_partial_update2;
