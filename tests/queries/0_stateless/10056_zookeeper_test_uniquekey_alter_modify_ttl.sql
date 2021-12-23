set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.alter_modify_ttl_t1;
drop table if exists test.alter_modify_ttl_t2;

CREATE table test.alter_modify_ttl_t1 (
    `d` Date,
    `id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/modify_ttl_test', 'r1')
partition by d
unique key id
order by id
ttl d + INTERVAL 1 DAY
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.alter_modify_ttl_t2 (
    `d` Date,
    `id` UInt64,
    `val` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/modify_ttl_test', 'r2')
partition by d
unique key id
order by id
ttl d + INTERVAL 1 DAY
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

-- Insert a past date, so it will be deleted #1
insert into test.alter_modify_ttl_t1 values ('2021-01-10', 10001, 1);

-- Insert a very far away future date, so it will not be ttled #2
insert into test.alter_modify_ttl_t1 values ('2100-07-23', 10002, 1);

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id, val from test.alter_modify_ttl_t1 order by id;
select 'r2', d, id, val from test.alter_modify_ttl_t2 order by id;

-- Alter table ttl from leader side
alter table test.alter_modify_ttl_t1 modify ttl d + INTERVAL 10 DAY;

-- Insert a past date, so it will be deleted
insert into test.alter_modify_ttl_t1 values ('2020-01-10', 10003, 1);

-- Insert a very far away future date, so it won't be ttled.
insert into test.alter_modify_ttl_t1 values ('2100-10-10', 10004, 1);

-- This raw should exist after modifying ttl.
insert into test.alter_modify_ttl_t1 values (toDate(now() - INTERVAL 8 DAY), 12345, 1);

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', id, val from test.alter_modify_ttl_t1 where d=toDate(now() - INTERVAL 8 DAY) order by id;
select 'r2', id, val from test.alter_modify_ttl_t2 where d=toDate(now() - INTERVAL 8 DAY) order by id;

select '-----------------------------------';
SHOW CREATE TABLE test.alter_modify_ttl_t1;
SHOW CREATE TABLE test.alter_modify_ttl_t2;

-- Alter table ttl from non-leader side
alter table test.alter_modify_ttl_t2 modify ttl d + INTERVAL 20 DAY;

select '-----------------------------------';
select sleep(3) format Null;
SHOW CREATE TABLE test.alter_modify_ttl_t1;
SHOW CREATE TABLE test.alter_modify_ttl_t2;

drop table if exists test.alter_modify_ttl_t1;
drop table if exists test.alter_modify_ttl_t2;

