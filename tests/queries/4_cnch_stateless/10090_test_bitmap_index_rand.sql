drop table if exists test_bitmap_local_test_rand;

CREATE TABLE test_bitmap_local_test_rand (`p_date` Date,`id` Int32,`vids` Array(Int32) BitmapIndex)
ENGINE = CnchMergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192, enable_build_ab_index = 1, min_bytes_for_wide_part = 0;

insert into test_bitmap_local_test_rand select '2022-01-01', rand(), [number / 3] from numbers(10);

select vids, arraySetCheck(vids, 2) from test_bitmap_local_test_rand order by vids settings enable_ab_index_optimization = 0;
select vids, arraySetCheck(vids, 2) from test_bitmap_local_test_rand order by vids settings enable_ab_index_optimization = 1;

drop table if exists test_bitmap_local_test_rand;