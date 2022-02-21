set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.alter_drop_column_t1;
drop table if exists test.alter_drop_column_t2;

CREATE table test.alter_drop_column_t1 (
    `d` Date,
    `id` UInt64,
    `col1` UInt32,
    `col2` UInt64,
    `col3` String,
    `col4` Date,
    `col5` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/drop_columns_test', 'r1')
partition by d
unique key id
order by id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.alter_drop_column_t2 (
    `d` Date,
    `id` UInt64,
    `col1` UInt32,
    `col2` UInt64,
    `col3` String,
    `col4` Date,
    `col5` UInt32)
ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/drop_columns_test', 'r2')
partition by d
unique key id
order by id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.alter_drop_column_t1 values ('2021-01-10', 10001, 1, 2, 'col3', '2021-01-12', 2);

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id, col1, col2, col3, col4, col5 from test.alter_drop_column_t1 order by id;
select 'r2', d, id, col1, col2, col3, col4, col5 from test.alter_drop_column_t2 order by id;

-- Alter table drop column
alter table test.alter_drop_column_t1 drop column col1;

-- Insert immediately after drop a column
insert into test.alter_drop_column_t1 values ('2021-01-10', 10002, 2, 'col3', '2021-01-12', 2);

-- Alter table drop column
alter table test.alter_drop_column_t1 drop column col2;

-- Insert immediately after drop a column
insert into test.alter_drop_column_t1 values ('2021-01-10', 10003, 'col3', '2021-01-12', 2);

-- Alter table drop column
alter table test.alter_drop_column_t1 drop column col3;

-- Insert immediately after drop a column
insert into test.alter_drop_column_t1 values ('2021-01-10', 10004, '2021-01-12', 2);

-- Alter table drop column
alter table test.alter_drop_column_t1 drop column col4;

-- Alter table drop column
alter table test.alter_drop_column_t1 drop column col5;

-- Drop key columns should raise exceptions
alter table test.alter_drop_column_t1 drop column d; -- { serverError 47 };
alter table test.alter_drop_column_t1 drop column id; -- { serverError 47 };

-- Drop a not exist column also raises an exception.
alter table test.alter_drop_column_t1 drop column not_exist; -- { serverError 10 };

select '-----------------------------------';
select sleep(3) format Null;
select 'r1', d, id from test.alter_drop_column_t1 order by id ;
select 'r2', d, id from test.alter_drop_column_t2 order by id;

select '-----------------------------------';
show create table test.alter_drop_column_t1;
show create table test.alter_drop_column_t2;

select '-----------------------------------';
select 'optimize table final';
optimize table test.alter_drop_column_t1 final;
select sleep(3) format Null;
select 'r1', d, id from test.alter_drop_column_t1 order by id ;
select 'r2', d, id from test.alter_drop_column_t2 order by id;

drop table if exists test.alter_drop_column_t1;
drop table if exists test.alter_drop_column_t2;

