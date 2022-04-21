set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.partition_level_unique_composite_key_r1;
drop table if exists test.partition_level_unique_composite_key_r2;
drop table if exists test.partition_level_unique_expression_key_r3;
drop table if exists test.partition_level_unique_expression_key_r4;

create table test.partition_level_unique_composite_key_r1 (d Date, id Int32, s String, revenue Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_composite_key', 'r1') partition by d order by id unique key (id, s)
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

create table test.partition_level_unique_composite_key_r2 (d Date, id Int32, s String, revenue Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_composite_key', 'r2') partition by d order by id unique key (id, s)
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

create table test.partition_level_unique_expression_key_r3 (d Date, id Int32, s String, revenue Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_expression_key', 'r3') partition by d order by id unique key (id, sipHash64(s))
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

create table test.partition_level_unique_expression_key_r4 (d Date, id Int32, s String, revenue Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/partition_level_unique_expression_key', 'r4') partition by d order by id unique key (id, sipHash64(s))
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

insert into test.partition_level_unique_composite_key_r1 values ('2020-10-29', 1001, '1001A', 100), ('2020-10-29', 1002, '1002A', 200), ('2020-10-29', 1001, '1001B', 300), ('2020-10-29', 1001, '1001A', 400);
insert into test.partition_level_unique_expression_key_r3 values ('2020-10-29', 1001, '1001A', 100), ('2020-10-29', 1002, '1002A', 200), ('2020-10-29', 1001, '1001B', 300), ('2020-10-29', 1001, '1001A', 400);

system sync replica test.partition_level_unique_composite_key_r2;
system sync replica test.partition_level_unique_expression_key_r4;

select 'r1', d, id, s, revenue from test.partition_level_unique_composite_key_r1 order by d, id, s;
select 'r2', d, id, s, revenue from test.partition_level_unique_composite_key_r2 order by d, id, s;
select 'r3', d, id, s, revenue from test.partition_level_unique_expression_key_r3 order by d, id, s;
select 'r4', d, id, s, revenue from test.partition_level_unique_expression_key_r4 order by d, id, s;

insert into test.partition_level_unique_composite_key_r2 values ('2020-10-29', 1002, '1002B', 500), ('2020-10-30', 1001, '1001A', 100), ('2020-10-30', 1002, '1002A', 200), ('2020-10-30', 1001, '1001B', 300);
insert into test.partition_level_unique_expression_key_r4 values ('2020-10-29', 1002, '1002B', 500), ('2020-10-30', 1001, '1001A', 100), ('2020-10-30', 1002, '1002A', 200), ('2020-10-30', 1001, '1001B', 300);

system sync replica test.partition_level_unique_composite_key_r2;
system sync replica test.partition_level_unique_expression_key_r4;

select 'r1', d, id, s, revenue from test.partition_level_unique_composite_key_r1 order by d, id, s;
select 'r2', d, id, s, revenue from test.partition_level_unique_composite_key_r2 order by d, id, s;
select 'r3', d, id, s, revenue from test.partition_level_unique_expression_key_r3 order by d, id, s;
select 'r4', d, id, s, revenue from test.partition_level_unique_expression_key_r4 order by d, id, s;

drop table if exists test.partition_level_unique_composite_key_r1;
drop table if exists test.partition_level_unique_composite_key_r2;
drop table if exists test.partition_level_unique_expression_key_r3;
drop table if exists test.partition_level_unique_expression_key_r4;
