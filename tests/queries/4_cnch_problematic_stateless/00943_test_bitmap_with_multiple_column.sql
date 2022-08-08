drop table if exists test.test_bitmap_index;

set enable_ab_index_optimization = 1;

create table if not exists test.test_bitmap_index (date Date, id Int32, version Int32, int_vid Array(Int32) BLOOM, float_vid Array(Float32) BLOOM, str_vid Array(String) BLOOM, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test.test_bitmap_index values ('2019-01-01', 1, 1, [1], [1], ['1'], [1]);
insert into test.test_bitmap_index values ('2019-01-01', 2, 2, [2], [2], ['2'], [2]);
insert into test.test_bitmap_index values ('2019-01-01', 3, 3, [3], [3], ['3'], [3]);
insert into test.test_bitmap_index values ('2019-01-02', 4, 4, [4], [4], ['4'], [4]);
insert into test.test_bitmap_index values ('2019-01-02', 5, 5, [5], [5], ['5'], [5]);

select count() from test.test_bitmap_index where arraySetCheck(int_vid, 1) and version = 1; -- 1
select version from test.test_bitmap_index where arraySetCheck(int_vid, 1) and version = 1; -- 1
select int_vid from test.test_bitmap_index where arraySetCheck(int_vid, 1) and version = 1; -- 1


select count() from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and version = 1; -- 0
select version from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and version = 1; 
select int_vid from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and version = 1;

select count() from test.test_bitmap_index where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2) and version = 1; -- 1
select version from test.test_bitmap_index where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2) and version = 1 order by version; 
select int_vid from test.test_bitmap_index where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2) and version = 1 order by int_vid;

select count() from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) or arraySetCheck(int_vid, 2) and version = 1; -- 0
select version from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) or arraySetCheck(int_vid, 2) and version = 1 order by version; 
select int_vid from test.test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) or arraySetCheck(int_vid, 2) and version = 1 order by int_vid;

drop table if exists test.test_bitmap_index;
