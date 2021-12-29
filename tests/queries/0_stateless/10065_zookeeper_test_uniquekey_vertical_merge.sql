set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;
set enable_disk_based_unique_key_index_method = 1;

drop table if exists test.unique_vertical_merge;

create table test.unique_vertical_merge (c1 Int32, c2 Int32, k1 UInt32, k2 String, v1 UInt32)
ENGINE=HaUniqueMergeTree('/clickhouse/tables/test/unique_vertical_merge', 'r1', v1)
order by (k1, intHash64(k1))
unique key (k1, k2)
sample by intHash64(k1)
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_columns_to_activate = 0, vertical_merge_algorithm_min_rows_to_activate = 0, enable_disk_based_unique_key_index = 1;

insert into test.unique_vertical_merge values (0, 100, 1, 'a', 1), (1, 101, 1, 'b', 1), (2, 102, 1, 'c', 1);
insert into test.unique_vertical_merge values (3, 103, 1, 'b', 1), (4, 104, 2, 'b', 1), (5, 105, 2, 'a', 1);
insert into test.unique_vertical_merge values (6, 106, 3, 'a', 1), (7, 107, 3, 'b', 1), (8, 108, 2, 'a', 1);

select '-- before merge --';
select * from test.unique_vertical_merge order by k1, k2;
select '#parts:', count(1) from system.parts where database='test' and table='unique_vertical_merge' and active;

optimize table test.unique_vertical_merge final;
select '-- after merge --';
select * from test.unique_vertical_merge order by k1, k2;
select '#parts:', count(1) from system.parts where database='test' and table='unique_vertical_merge' and active;

drop table if exists test.unique_vertical_merge;
