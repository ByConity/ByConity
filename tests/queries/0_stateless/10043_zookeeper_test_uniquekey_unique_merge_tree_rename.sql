set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.unique_rename_before;
drop table if exists test.unique_rename_after;

create table test.unique_rename_before (d Date, id Int32, m Int32) ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_rename_before', 'r1') partition by d order by id unique key id;

insert into test.unique_rename_before values ('2020-10-28', 1, 10), ('2020-10-28', 2, 20), ('2020-10-29', 1, 11);

select 'before', d, id, m from test.unique_rename_before order by d, id;

rename table test.unique_rename_before to test.unique_rename_after;

select 'before', d, id, m from test.unique_rename_before order by d, id; -- { serverError 60 }
select 'after', d, id, m from test.unique_rename_after order by d, id;

insert into test.unique_rename_after values ('2020-10-29', 1, 100), ('2020-10-29', 2, 200);

select 'after', d, id, m from test.unique_rename_after order by d, id;

drop table if exists test.unique_rename_before;
drop table if exists test.unique_rename_after;
