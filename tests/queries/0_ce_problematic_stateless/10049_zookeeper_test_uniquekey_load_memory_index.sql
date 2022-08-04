set database_atomic_wait_for_drop_and_detach_synchronously = 1;
set max_insert_wait_seconds_for_unique_table_leader = 30;

drop table if exists test.load_memory_index_filter;

create table test.load_memory_index_filter ( `d` Date, `k` UInt64, `v` UInt64) ENGINE = HaUniqueMergeTree('/clickhouse/tables/test/test_load_memory_index_filter', 'r1') partition by d unique key k order by k SETTINGS cleanup_delay_period=1000, enable_disk_based_unique_key_index=0;

-- insert originl data to create part-#0
insert into test.load_memory_index_filter values ('2021-01-10', 0, 0), ('2021-01-10', 1, 1), ('2021-01-10', 2, 2),

-- upsert first time: uses empty delete bitmap for part-#0
insert into test.load_memory_index_filter values ('2021-01-10', 0, 10);

select unique_index_size from system.parts where database='test' and table='load_memory_index_filter' and name='20210110_0_0_0';

-- upsert the second time
insert into test.load_memory_index_filter values ('2021-01-10', 1, 11);

alter table test.load_memory_index_filter detach partition '2021-01-10';

-- attach to test load memory index with delete bitmap
alter table test.load_memory_index_filter attach partition '2021-01-10';

select unique_index_size from system.parts where database='test' and table='load_memory_index_filter' and name='20210110_0_0_0';

select d, k, v from  test.load_memory_index_filter order by k;

drop table if exists test.load_memory_index_filter;
