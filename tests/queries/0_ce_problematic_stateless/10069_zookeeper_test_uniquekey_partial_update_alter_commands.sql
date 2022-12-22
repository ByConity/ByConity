set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_unique_partial_update = 1;

drop table if exists unique_partial_update_alter1;
drop table if exists unique_partial_update_alter2;

-- test partial update mode, disable unique row store
CREATE table unique_partial_update_alter1(
    `id` UInt64,
    `s1` String,
    `a1` Array(Int32),
    `m1` Map(String, Int32))
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10069_zookeeper_test_uniquekey_partial_update_alter_commands/unique_partial_update_alter', 'r1')
order by id
unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0;

CREATE table unique_partial_update_alter2(
    `id` UInt64,
    `s1` String,
    `a1` Array(Int32),
    `m1` Map(String, Int32))
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10069_zookeeper_test_uniquekey_partial_update_alter_commands/unique_partial_update_alter', 'r2')
order by id
unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 0, replicated_can_become_leader=0;

insert into unique_partial_update_alter1 values (1, '', [1, 2], {'k1': 1, 'k2': 2}), (2, 'abc', [3, 4 ,5], {'k3': 3, 'k4': 4});
insert into unique_partial_update_alter1 values (1, 'bce', [10, 20], {'k1': 10, 'k5': 5}), (2, '', [], {'k7': 7, 'k8': 8});

select 'insert with partial update mode, disbale unique row store';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

select '';
alter table unique_partial_update_alter1 drop column a1, add column a2 Array(Int32);
insert into unique_partial_update_alter1 values (1, '', {'k5': 50, 'k9': 9}, [100, 200]), (3, 'def', {'k3': 4, 'k4': 3}, [30, 40, 50]);
optimize table unique_partial_update_alter2 final;
select 'drop column a1, update one row and insert one new row, then merge';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

select '';
insert into unique_partial_update_alter2 values (2, 'bcd', {'k5': 5}, [10, 20]), (5, 'efg', {'k7': 7}, [70, 80]);
select 'update 1 row and insert one row';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

drop table if exists unique_partial_update_alter1;
drop table if exists unique_partial_update_alter2;

select '';
-- test partial update mode, enable unique row store
CREATE table unique_partial_update_alter1(
    `id` UInt64,
    `s1` String,
    `a1` Array(Int32),
    `m1` Map(String, Int32))
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10069_zookeeper_test_uniquekey_partial_update_alter_commands/unique_partial_update_alter', 'r1')
order by id
unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1;

CREATE table unique_partial_update_alter2(
    `id` UInt64,
    `s1` String,
    `a1` Array(Int32),
    `m1` Map(String, Int32))
ENGINE = HaUniqueMergeTree('/clickhouse/tables/10069_zookeeper_test_uniquekey_partial_update_alter_commands/unique_partial_update_alter', 'r2')
order by id
unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, enable_unique_partial_update = 1, enable_unique_row_store = 1, replicated_can_become_leader=0;

insert into unique_partial_update_alter1 values (1, '', [1, 2], {'k1': 1, 'k2': 2}), (2, 'abc', [3, 4 ,5], {'k3': 3, 'k4': 4});
insert into unique_partial_update_alter1 values (1, 'bce', [10, 20], {'k1': 10, 'k5': 5}), (2, '', [], {'k7': 7, 'k8': 8});

select 'insert with partial update mode, enable unique row store';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

select '';
alter table unique_partial_update_alter1 drop column a1, add column a2 Array(Int32);
insert into unique_partial_update_alter1 values (1, '', {'k5': 50, 'k9': 9}, [100, 200]), (3, 'def', {'k3': 4, 'k4': 3}, [30, 40, 50]);
optimize table unique_partial_update_alter2 final;
select 'drop column a1, update one row and insert one new row, then merge, trigger generating row store from storage';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

select '';
insert into unique_partial_update_alter2 values (2, 'bcd', {'k5': 5}, [10, 20]), (5, 'efg', {'k7': 7}, [70, 80]);
select 'update 1 row and insert one row';
select 'select ha unique table r1';
select * from unique_partial_update_alter1 order by id;
system sync replica unique_partial_update_alter2;
select 'select ha unique table r2';
select * from unique_partial_update_alter2 order by id;

drop table if exists unique_partial_update_alter1;
drop table if exists unique_partial_update_alter2;
