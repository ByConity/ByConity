set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.SOURCE_insert_unique_by_mv;
drop table if exists test.VIEW_insert_unique_by_mv;
drop table if exists test.insert_unique_by_mv1;
drop table if exists test.insert_unique_by_mv2;

-- NOTE: for this test, we use the community Map implemention instead our internal's.
create table test.insert_unique_by_mv1 (d Date, id Int32, params Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/test/insert_unique_by_mv', 'r1') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;
create table test.insert_unique_by_mv2 (d Date, id Int32, params Map(String, Int32)) Engine=HaUniqueMergeTree('/clickhouse/tables/test/insert_unique_by_mv', 'r2') order by id unique key id SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;
create table test.SOURCE_insert_unique_by_mv (c1 Int32, c2 String, c3 Map(String, Int32)) Engine=MergeTree order by c1;
create materialized view test.VIEW_insert_unique_by_mv to test.insert_unique_by_mv1 (id Int32, d Date, params Map(String, Int32)) as select c1 as id, c2 as d, c3 as params from test.SOURCE_insert_unique_by_mv;

insert into test.SOURCE_insert_unique_by_mv values (1, '2020-12-01', {'k1':11,'k2':21}), (2, '2020-12-02', {'k2':22,'k3':32});
select sleep(3) format Null;
select 'r1', d, id, params{'k1'}, params{'k2'}, params{'k3'} from test.insert_unique_by_mv1 order by id;

drop table test.VIEW_insert_unique_by_mv;
create materialized view test.VIEW_insert_unique_by_mv to test.insert_unique_by_mv2 (id Int32, d Date, params Map(String, Int32)) as select c1 as id, c2 as d, c3 as params from test.SOURCE_insert_unique_by_mv;
insert into test.SOURCE_insert_unique_by_mv values (2, '2020-12-03', {'k2':100}), (3, '2020-12-04', {'k1':13,'k2':23});
select sleep(3) format Null;
select 'r2', d, id, params{'k1'}, params{'k2'}, params{'k3'} from test.insert_unique_by_mv2 order by id;

drop table if exists test.SOURCE_insert_unique_by_mv;
drop table if exists test.VIEW_insert_unique_by_mv;
drop table if exists test.insert_unique_by_mv1;
drop table if exists test.insert_unique_by_mv2;
