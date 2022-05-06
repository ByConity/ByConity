drop table if exists test.test_bitmap_index_single;

create table if not exists test.test_bitmap_index_single (date Date, id Int32, int_vid Array(Int32) BitmapIndex, float_vid Array(Float32) BitmapIndex, ext Array(Int32)) engine = MergeTree partition by date order by id settings min_bytes_for_wide_part = 0;

insert into test.test_bitmap_index_single values ('2019-01-01', 1, [1], [1], [1]);
insert into test.test_bitmap_index_single values ('2019-01-01', 2, [2], [2], [2]);
insert into test.test_bitmap_index_single values ('2019-01-01', 3, [3], [3], [3]);
insert into test.test_bitmap_index_single values ('2019-01-02', 4, [4], [4], [4]);
insert into test.test_bitmap_index_single values ('2019-01-02', 5, [5], [5], [5]);

select id, arraySetGetAny(int_vid, (1)) from test.test_bitmap_index_single order by id settings enable_ab_index_optimization = 1;
select id, arraySetGetAny(float_vid, (1)) from test.test_bitmap_index_single order by id settings enable_ab_index_optimization = 1; -- { serverError 43}
select id, arraySetGetAny(ext, (1)) from test.test_bitmap_index_single order by id settings enable_ab_index_optimization = 1;

drop table if exists test.test_bitmap_index_single;