set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.test_unique_merge_1;
drop table if exists test.test_unique_merge_2;
drop table if exists test.test_unique_merge_3;

CREATE table test.test_unique_merge_1 ( `d` Date, `k` UInt64, `v` UInt64) ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_unique_merge', 'r1') partition by d unique key k order by k SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

select sleep(3) format Null;

CREATE table test.test_unique_merge_2 (`d` Date, `k` UInt64, `v` UInt64) ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_unique_merge', 'r2')
partition by d unique key k order by k SETTINGS ha_unique_update_log_sleep_ms=10, ha_unique_replay_log_sleep_ms=10;

insert into test.test_unique_merge_1 values ('2021-01-10', 0, 0), ('2021-01-10', 1, 1), ('2021-01-10', 2, 2),
insert into test.test_unique_merge_1 values ('2021-01-10', 0, 10), ('2021-01-10', 1, 11);

-- Run merge
optimize table test.test_unique_merge_1 partition '2021-01-10';

select sleep(3) format Null;

-- Expect all small parts get merged into a big part containing all rows.
select '-- merged result --';
select level > 0 and rows == 3 as result from system.parts where database='test' and table='test_unique_merge_1' group by result order by result;

select '-- merged data --';
select d, k, v from test.test_unique_merge_2;

-- Test use expression as unique key
create table test.test_unique_merge_3 (id Int32, a Int32) Engine=HaUniqueMergeTree('/clickhouse/tables/test/test_unique_merge_3', 'r1') order by id unique key sipHash64(id);
select sleep(3) format Null;
insert into test.test_unique_merge_3 values (1, 2);
insert into test.test_unique_merge_3 values (2, 3);
optimize table test.test_unique_merge_3 final;

drop table if exists test.test_unique_merge_1;
drop table if exists test.test_unique_merge_2;
drop table if exists test.test_unique_merge_3;
