set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_repair_r1;
drop table if exists test.unique_repair_r2;

create table test.unique_repair_r1 (d Date, id Int32, m Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_repair', 'r1') partition by d order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.unique_repair_r1 values ('2020-10-28', 1, 10);
insert into test.unique_repair_r1 values ('2020-10-28', 2, 20);
insert into test.unique_repair_r1 values ('2020-10-29', 1, 11);
insert into test.unique_repair_r1 values ('2020-10-29', 2, 21);

create table test.unique_repair_r2 (d Date, id Int32, m Int32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_repair', 'r2') partition by d order by id unique key id
SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10, replicated_can_become_leader=0;

insert into test.unique_repair_r1 values ('2020-10-30', 1, 12);
insert into test.unique_repair_r1 values ('2020-10-30', 2, 22);

system sync replica test.unique_repair_r2;

select table, is_leader, is_readonly, status from system.ha_unique_replicas where database='test' and table='unique_repair_r1';
select table, is_leader, is_readonly, status from system.ha_unique_replicas where database='test' and table='unique_repair_r2';
select 'r1', d, id, m from test.unique_repair_r1 order by d, id;
select 'r2', d, id, m from test.unique_repair_r1 order by d, id;

drop table if exists test.unique_repair_r1;
drop table if exists test.unique_repair_r2;
