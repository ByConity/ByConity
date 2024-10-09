drop table if exists test_bitmap_index_output;

set enable_ab_index_optimization = 1;

create table if not exists test_bitmap_index_output (date Date, id Int32, int_vid Array(Int32) BitmapIndex, float_vid Array(Float32) BitmapIndex, str_vid Array(String) BitmapIndex, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test_bitmap_index_output values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test_bitmap_index_output values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test_bitmap_index_output values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test_bitmap_index_output values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test_bitmap_index_output values ('2019-01-02', 5, [5], [5], ['5'], [5]);

select 'output array';
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 4) order by id; --g2
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 4) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 4) order by id; --g3
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, (1,2)) and arraySetCheck(int_vid, 4) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, (1,4)) and arraySetCheck(int_vid, 4) order by id; --g1
select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 3)) and arraySetCheck(int_vid, 4) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1) order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1) and id = 1 order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, (1, 2)) and arraySetCheck(ext, 1) and id = 1 order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and id = 1 order by id; --g1
select int_vid from test_bitmap_index_output where not arraySetCheck(int_vid, 1) and id = 1 order by id; --g0
select int_vid from test_bitmap_index_output where not arraySetCheck(int_vid, 1) order by id; --g4
select int_vid from test_bitmap_index_output where not arraySetCheck(int_vid, 1) and not arraySetCheck(int_vid, 2) order by id; --g3 
select int_vid from test_bitmap_index_output where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) order by id; --g4

select int_vid from test_bitmap_index_output where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3) order by id; --g1
select int_vid from test_bitmap_index_output where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3) order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and arraySetCheck(int_vid, 3) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) order by id; --g1
select int_vid from test_bitmap_index_output where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) order by id; --g4
select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) or arraySetCheck(int_vid, 2) order by id; --g2
select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2) order by id; --g1
select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2) order by id; --g1
-- maybe support later
-- select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1, int_vid, 2)) or arraySetCheck(int_vid, 2) order by id; --g1
-- select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1, int_vid, 2)) and arraySetCheck(int_vid, 2) order by id; --g0
-- select int_vid from test_bitmap_index_output where (arraySetCheck(int_vid, 1, float_vid, 1)) and arraySetCheck(int_vid, 2, float_vid, 2) order by id; --g0
-- select int_vid from test_bitmap_index_output where arraySetCheck(int_vid, 1, float_vid, 1, str_vid, '1') order by id; --g1

select int_vid, str_vid from test_bitmap_index_output where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 4) order by id; --g0
select int_vid, float_vid from test_bitmap_index_output where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1) order by id; --g1
select int_vid, str_vid, float_vid from test_bitmap_index_output where arraySetCheck(str_vid, ('1','2')) or arraySetCheck(float_vid, 4) order by id; --g3
select int_vid from test_bitmap_index_output where arraySetCheck(str_vid, ('1','2')) and arraySetCheck(float_vid, 4) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(str_vid, ('1','4')) and arraySetCheck(float_vid, 4) order by id; --g1
select int_vid from test_bitmap_index_output where (arraySetCheck(str_vid, ('1','2')) or arraySetCheck(str_vid, '3')) and arraySetCheck(float_vid, 4) order by id; --g0
select int_vid from test_bitmap_index_output where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1) order by id; --g1
select int_vid from test_bitmap_index_output where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1) and id = 1 order by id; --g1
select int_vid from test_bitmap_index_output where not arraySetCheck(str_vid, '1') and not arraySetCheck(float_vid, 2) order by id; --g3
select int_vid from test_bitmap_index_output where not (arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 2)) order by id; --g4
select int_vid, str_vid from test_bitmap_index_output where not ((arraySetCheck(str_vid, '1') or arraySetCheck(str_vid, '2')) and arraySetCheck(float_vid, 3)) order by id; --g1

drop table if exists test_bitmap_index_output;