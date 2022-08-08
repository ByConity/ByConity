set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_optimize_r1;
drop table if exists test.unique_optimize_r2;

create table test.unique_optimize_r1 (d Date, id Int32, m Int32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_optimize', 'r1') partition by d order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;
create table test.unique_optimize_r2 (d Date, id Int32, m Int32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_optimize', 'r2') partition by d order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.unique_optimize_r1 values ('2020-10-28', 1, 10);
insert into test.unique_optimize_r1 values ('2020-10-28', 2, 20);
insert into test.unique_optimize_r1 values ('2020-10-29', 1, 11);
insert into test.unique_optimize_r1 values ('2020-10-29', 2, 21);
insert into test.unique_optimize_r1 values ('2020-10-30', 1, 12);
insert into test.unique_optimize_r1 values ('2020-10-30', 2, 22);

optimize table test.unique_optimize_r1 partition '2020-10-28';

select table, name from system.parts where database='test' and table='unique_optimize_r1' and active=1 order by name;
system sync replica test.unique_optimize_r2 ;
select table, name from system.parts where database='test' and table='unique_optimize_r2' and active=1 order by name;
select * from test.unique_optimize_r1 order by d, id;

optimize table test.unique_optimize_r2 final;

select table, name from system.parts where database='test' and table='unique_optimize_r1' and active=1 order by name;
system sync replica test.unique_optimize_r2 ;
select table, name from system.parts where database='test' and table='unique_optimize_r2' and active=1 order by name;
select * from test.unique_optimize_r1 order by d, id;

drop table if exists test.unique_optimize_r1;
drop table if exists test.unique_optimize_r2;
