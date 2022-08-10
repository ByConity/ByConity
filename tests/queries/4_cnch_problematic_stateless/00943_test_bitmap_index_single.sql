drop table if exists test.test_bitmap_index;

create table if not exists test.test_bitmap_index (date Date, id Int32, int_vid Array(Int32) BLOOM, float_vid Array(Float32) BLOOM, str_vid Array(String) BLOOM, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test.test_bitmap_index values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test.test_bitmap_index values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test.test_bitmap_index values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test.test_bitmap_index values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test.test_bitmap_index values ('2019-01-02', 5, [5], [5], ['5'], [5]);

select id, arraySetGetAny(int_vid, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 0;
select id, arraySetGetAny(float_vid, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 0; -- { serverError 43}
select id, arraySetGetAny(str_vid, ('1')) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 0; -- { serverError 43}
select id, arraySetGetAny(ext, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 0;

select id, arraySetGetAny(int_vid, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 1;
select id, arraySetGetAny(float_vid, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 1; -- { serverError 43}
select id, arraySetGetAny(str_vid, ('1')) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 1; -- { serverError 43}
select id, arraySetGetAny(ext, (1)) from test.test_bitmap_index order by id settings enable_ab_index_optimization = 1;

drop table if exists test.test_bitmap_index;