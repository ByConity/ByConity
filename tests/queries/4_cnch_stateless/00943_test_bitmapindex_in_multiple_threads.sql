drop table if exists test_bitmap_index_397843;

CREATE TABLE test_bitmap_index_397843 (`p_date` Date, `id` Int32, `vids` Array(Int32) BitMapIndex) ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id SETTINGS enable_build_ab_index = 1, index_granularity = 8192;

insert into test_bitmap_index_397843 select '2023-01-01', number, [number % 10] from numbers(100000);

select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, max_threads = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, enable_ab_index_optimization = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, enable_ab_index_optimization = 0;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 0, enable_ab_index_optimization = 0;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 0, enable_ab_index_optimization = 1;

alter table test_bitmap_index_397843 modify setting enable_build_ab_index = 0;

insert into test_bitmap_index_397843 select '2023-01-01', number, [number % 10] from numbers(100000);

select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, max_threads = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, enable_ab_index_optimization = 1;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 1, enable_ab_index_optimization = 0;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 0, enable_ab_index_optimization = 0;
select count() from test_bitmap_index_397843 where arraySetCheck(vids, 3) settings enable_optimizer = 0, enable_ab_index_optimization = 1;

drop table if exists test_bitmap_index_397843;
