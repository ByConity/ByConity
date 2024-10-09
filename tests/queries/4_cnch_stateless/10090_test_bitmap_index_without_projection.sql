drop table if exists test_bitmap_index_without_projection;

set optimizer_index_projection_support = 0;
set enable_ab_index_optimization = 1;
set enable_optimizer = 1;

set max_threads=8;
set exchange_source_pipeline_threads = 1;

create table if not exists test_bitmap_index_without_projection (p_date Date, id Int32, vids Array(Int32) BitmapIndex) 
    engine = CnchMergeTree partition by p_date order by id settings min_bytes_for_wide_part = 0, enable_build_ab_index = 0;

insert into test_bitmap_index_without_projection select '2023-01-01', number, [number % 3] from numbers(5);

-- Do not add explain pipeline cause the number of worker for ci is changeable

select '1 part without index';
-- explain pipeline 
-- select sum(id) from test_bitmap_index_without_projection
--     where arraySetCheck(vids, 1);

select sum(id) from test_bitmap_index_without_projection
    where arraySetCheck(vids, 1);

alter table test_bitmap_index_without_projection modify setting enable_build_ab_index = 1;

insert into test_bitmap_index_without_projection select '2023-01-01', number, [number % 3] from numbers(5);

select '1 part without index, 1 part with index';
-- explain pipeline 
-- select sum(id) from test_bitmap_index_without_projection
--     where arraySetCheck(vids, 1);

select sum(id) from test_bitmap_index_without_projection
    where arraySetCheck(vids, 1);


select 'select arraySetCheck';

-- explain pipeline 
-- select arraySetCheck(vids, 1) from test_bitmap_index_without_projection
--     where arraySetCheck(vids, 1);

select arraySetCheck(vids, 1) from test_bitmap_index_without_projection
    where arraySetCheck(vids, 1);

-- explain pipeline 
-- select sum(arraySetCheck(vids, 1)) from test_bitmap_index_without_projection
--     where arraySetCheck(vids, 1);

select sum(arraySetCheck(vids, 1)) from test_bitmap_index_without_projection
    where arraySetCheck(vids, 1);

drop table if exists test_bitmap_index_without_projection;
