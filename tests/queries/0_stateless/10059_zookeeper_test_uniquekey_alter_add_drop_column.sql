set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.alter_add_drop_column_t1;
drop table if exists test.alter_add_drop_column_t2;

CREATE table test.alter_add_drop_column_t1 (
    `d` Date,
    `id` UInt64
)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/add_drop_columns', 'r1')
partition by d
unique key id
order by id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.alter_add_drop_column_t2 (
    `d` Date,
    `id` UInt64
)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/add_drop_columns', 'r2')
partition by d
unique key id
order by id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

-- Insert a initial record #1
insert into test.alter_add_drop_column_t1 values ('2021-01-10', 10001);

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id from test.alter_add_drop_column_t1 order by id;
select 'r2', d, id from test.alter_add_drop_column_t2 order by id;

-- Alter table add column
alter table test.alter_add_drop_column_t1 add column col1 UInt32;

-- Alter table add column
alter table test.alter_add_drop_column_t1 add column col2 UInt64;

-- Alter table add column
alter table test.alter_add_drop_column_t1 add column col3 Date;

-- Alter table add column
alter table test.alter_add_drop_column_t1 add column col4 UInt32;

-- Insert a new record #2
insert into test.alter_add_drop_column_t1 values ('2022-02-22', 10002, 1, 2, '2021-01-12', 2);

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id, col1, col2, col3, col4 from test.alter_add_drop_column_t1 order by id;
select 'r2', d, id, col1, col2, col3, col4 from test.alter_add_drop_column_t2 order by id;

-- Alter table drop column
alter table test.alter_add_drop_column_t1 drop column col1;

-- Alter table drop column
alter table test.alter_add_drop_column_t1 drop column col2;

-- Alter table drop column
alter table test.alter_add_drop_column_t1 drop column col3;

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id, col4 from test.alter_add_drop_column_t1 order by id;
select 'r2', d, id, col4 from test.alter_add_drop_column_t2 order by id;

select '-----------------------------------';
show create table test.alter_add_drop_column_t1;
show create table test.alter_add_drop_column_t2;

drop table if exists test.alter_add_drop_column_t1;
drop table if exists test.alter_add_drop_column_t2;

