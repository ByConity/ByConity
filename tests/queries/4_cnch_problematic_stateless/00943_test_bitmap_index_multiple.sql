drop table if exists test_bitmap_index;

create table if not exists test_bitmap_index (date Date, id Int32, int_vid Array(Int32) BitmapIndex, float_vid Array(Float32) BitmapIndex, str_vid Array(String) BitmapIndex, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test_bitmap_index values ('2019-01-01', 1, [1], [1], ['1'], [1,2]);
insert into test_bitmap_index values ('2019-01-01', 2, [2], [2], ['2'], [2,3]);
insert into test_bitmap_index values ('2019-01-01', 3, [3], [3], ['3'], [3,4]);
insert into test_bitmap_index values ('2019-01-02', 4, [4], [4], ['4'], [4,5]);
insert into test_bitmap_index values ('2019-01-02', 5, [5], [5], ['5'], [5,6]);

select id, arraySetGet(int_vid, (1)) from test_bitmap_index order by id settings enable_ab_index_optimization = 0;
select id, arraySetGet(float_vid, (1)) from test_bitmap_index order by id settings enable_ab_index_optimization = 0; -- { serverError 43}
select id, arraySetGet(str_vid, ('1')) from test_bitmap_index order by id settings enable_ab_index_optimization = 0; -- { serverError 43}
select id, arraySetGet(ext, (1,2,3,4)) from test_bitmap_index order by id settings enable_ab_index_optimization = 0;

select id, arraySetGet(int_vid, (1)) from test_bitmap_index order by id settings enable_ab_index_optimization = 1;
select id, arraySetGet(float_vid, (1)) from test_bitmap_index order by id settings enable_ab_index_optimization = 1; -- { serverError 43}
select id, arraySetGet(str_vid, ('1')) from test_bitmap_index order by id settings enable_ab_index_optimization = 1; -- { serverError 43}
select id, arraySetGet(ext, (1,2,3,4)) from test_bitmap_index order by id settings enable_ab_index_optimization = 1;

drop table if exists test_bitmap_index;