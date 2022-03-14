set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_unique_partial_update = 1;

drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;

------- test constrains of partial update features
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, partition_level_unique_keys = 0;  -- { serverError 49 }
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r1') order by id unique key (id, a) SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, partition_level_unique_keys = 1;  -- { serverError 49 }

------- test normal insert operation
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 0, enable_unique_row_store = 0;
create table uniquekey_partial_update2 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 0, enable_unique_row_store = 0;

insert into uniquekey_partial_update1 values 
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});
insert into uniquekey_partial_update1 (id, b, e) values (1, 'x', {'h': 12}), (1, '', {'c': 5}), (2, '7', {'f': 10}), (2, '8', {'g': 14});
select sleep(3) format Null;
select 'insert with normal mode, using default value';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

optimize table uniquekey_partial_update1 final;
insert into uniquekey_partial_update1 (id, a, d) values (1, 4, [10, 20]), (2, 6, [50, 60]);
select sleep(3) format Null;
select 'after merge, insert with normal mode';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;

select '';
------- test partial update insert operation, disable unique_row_store
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;
create table uniquekey_partial_update2 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

insert into uniquekey_partial_update1 values 
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});
insert into uniquekey_partial_update1 (id, b, e) values (1, 'x', {'h': 12}), (1, '', {'c': 5}), (2, '7', {'f': 10}), (2, '8', {'g': 14});
select sleep(3) format Null;
select 'insert with partial update mode, disable unique row store';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

optimize table uniquekey_partial_update1 final;
insert into uniquekey_partial_update1 (id, a, d) values (1, 4, [10, 20]), (2, 6, [50, 60]);
select sleep(3) format Null;
select 'after merge, insert with partial update mode, disable unique row store';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;

select '';
------- test partial update insert operation, enable unique_row_store
create table uniquekey_partial_update1 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;
create table uniquekey_partial_update2 (id Int32, a Int32, b LowCardinality(String), c FixedString(10), d Array(Int32), e Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/10066_zookeeper_test_uniquekey_partial_update/uniquekey_partial_update', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

insert into uniquekey_partial_update1 values 
(1, 0, 'x', 'x', [1, 2, 3], {'a': 1}), (1, 0, 'z', 'z', [1, 2], {'b': 3, 'c': 4}), (1, 2, 'm', 'm', [], {'a': 2, 'd': 7}), (2, 0, 'q', 'q', [], {'a': 1}), (2, 0, 't', 't', [4, 5, 6], {'e': 8}), (2, 3, '',  '',  [5, 6], {'e': 6, 'f': 9});
insert into uniquekey_partial_update1 (id, b, e) values (1, 'x', {'h': 12}), (1, '', {'c': 5}), (2, '7', {'f': 10}), (2, '8', {'g': 14});
select sleep(3) format Null;
select 'insert with partial update mode, enable unique row store';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

optimize table uniquekey_partial_update1 final;
insert into uniquekey_partial_update1 (id, a, d) values (1, 4, [10, 20]), (2, 6, [50, 60]);
select sleep(3) format Null;
select 'after merge, insert with partial update mode, enable unique row store';
select 'select replica 1';
select * from uniquekey_partial_update1;
select 'select replica 2';
select * from uniquekey_partial_update2;

drop table if exists uniquekey_partial_update1;
drop table if exists uniquekey_partial_update2;
