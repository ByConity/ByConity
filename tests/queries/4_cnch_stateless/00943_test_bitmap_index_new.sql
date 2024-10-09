drop table if exists test_bitmap_index_new;

set enable_ab_index_optimization = 1;

create table if not exists test_bitmap_index_new (date Date, id Int32, int_vid Array(Int32) BitmapIndex, float_vid Array(Float32) BitmapIndex, str_vid Array(String) BitmapIndex, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test_bitmap_index_new values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test_bitmap_index_new values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test_bitmap_index_new values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test_bitmap_index_new values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test_bitmap_index_new values ('2019-01-02', 5, [5], [5], ['5'], [5]);

select 'int_vid';
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 4); -- 2
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 4); -- 3
select count() from test_bitmap_index_new where arraySetCheck(int_vid, (1,2)) and arraySetCheck(int_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(int_vid, (1,4)) and arraySetCheck(int_vid, 4); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 3)) and arraySetCheck(int_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, (1, 2)) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where not arraySetCheck(int_vid, 1) and id = 1; -- 0
select count() from test_bitmap_index_new where not arraySetCheck(int_vid, 1); -- 4
select count() from test_bitmap_index_new where not arraySetCheck(int_vid, 1) and not arraySetCheck(int_vid, 2); -- 3 
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)); -- 4

select 'variadic';
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3); -- 1
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3); -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and arraySetCheck(int_vid, 3); -- 0
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1); -- 1
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)); -- 4
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) or arraySetCheck(int_vid, 2); -- 2
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2); -- 1
-- maybe support later
-- select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1, int_vid, 2)) or arraySetCheck(int_vid, 2); -- 1
-- select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1, int_vid, 2)) and arraySetCheck(int_vid, 2); -- 0
-- select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1, float_vid, 1)) and arraySetCheck(int_vid, 2, float_vid, 2); -- 0
-- select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1, float_vid, 1, str_vid, '1'); -- 1


select 'float_vid';
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 4); -- 2
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(float_vid, (1,2)) or arraySetCheck(float_vid, 4); -- 3
select count() from test_bitmap_index_new where arraySetCheck(float_vid, (1,2)) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(float_vid, (1,4)) and arraySetCheck(float_vid, 4); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(float_vid, (1,2)) or arraySetCheck(float_vid, 3)) and arraySetCheck(float_vid, 4); -- 0 
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) and arraySetCheck(float_vid, (1, 2)) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where arraySetCheck(float_vid, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where not arraySetCheck(float_vid, 1) and id = 1; -- 0
select count() from test_bitmap_index_new where not arraySetCheck(float_vid, 1); -- 4
select count() from test_bitmap_index_new where not arraySetCheck(float_vid, 1) and not arraySetCheck(float_vid, 2); -- 3  
select count() from test_bitmap_index_new where not (arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 2)); -- 4
select count() from test_bitmap_index_new where not ((arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 2)) and arraySetCheck(float_vid, 3)); -- 1

select 'str_vid';
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1'); -- 1
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') or arraySetCheck(str_vid, '4'); -- 2
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(str_vid, '4'); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','2')) or arraySetCheck(str_vid, '4'); -- 3 
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','2')) and arraySetCheck(str_vid, '4'); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','4')) and arraySetCheck(str_vid, '4'); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(str_vid, ('1','2')) or arraySetCheck(str_vid, '3')) and arraySetCheck(str_vid, '4'); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(ext, 1) and id = 1; -- 1 
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(str_vid, ('1', '2')) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and id = 1; -- 1
select count() from test_bitmap_index_new where not arraySetCheck(str_vid, '1') and id = 1; -- 0
select count() from test_bitmap_index_new where not arraySetCheck(str_vid, '1'); -- 4
select count() from test_bitmap_index_new where not arraySetCheck(str_vid, '1') and not arraySetCheck(str_vid, '2'); -- 3
select count() from test_bitmap_index_new where not (arraySetCheck(str_vid, '1') or arraySetCheck(str_vid, '2')); -- 4
select count() from test_bitmap_index_new where not ((arraySetCheck(str_vid, '1') or arraySetCheck(str_vid, '2')) and arraySetCheck(str_vid, '3')); -- 1

select 'mix_vid';
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','2')) or arraySetCheck(float_vid, 4); -- 3
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','2')) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, ('1','4')) and arraySetCheck(float_vid, 4); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(str_vid, ('1','2')) or arraySetCheck(str_vid, '3')) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1); -- 1
select count() from test_bitmap_index_new where arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 1) and id = 1; -- 1
select count() from test_bitmap_index_new where not arraySetCheck(str_vid, '1') and not arraySetCheck(float_vid, 2); -- 3
select count() from test_bitmap_index_new where not (arraySetCheck(str_vid, '1') and arraySetCheck(float_vid, 2)); -- 4
select count() from test_bitmap_index_new where not ((arraySetCheck(str_vid, '1') or arraySetCheck(str_vid, '2')) and arraySetCheck(float_vid, 3)); -- 1

select 'variadic mixed';
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) and arraySetCheck(float_vid, 3); -- 1
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) and arraySetCheck(float_vid, 3); -- 1
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and arraySetCheck(float_vid, 3); -- 0
select count() from test_bitmap_index_new where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 1); -- 1
select count() from test_bitmap_index_new where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 1)); -- 4
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) or arraySetCheck(int_vid, 2); -- 2
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(float_vid, 2); -- 1
select count() from test_bitmap_index_new where (arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 2)) or arraySetCheck(int_vid, 2); -- 1

drop table if exists test_bitmap_index_new;
