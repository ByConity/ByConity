drop table if exists test_bitmap_index;

set enable_ab_index_optimization = 1;

create table if not exists test_bitmap_index (date Date, id Int32, int_vid Array(Int32) BLOOM, float_vid Array(Float32) BLOOM, str_vid Array(String) BLOOM, ext Array(Int32)) engine = CnchMergeTree partition by date order by id settings min_bytes_for_wide_part = 0, enable_late_materialize=1;

insert into test_bitmap_index values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test_bitmap_index values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test_bitmap_index values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test_bitmap_index values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test_bitmap_index values ('2019-01-02', 5, [5], [5], ['5'], [5]);

-- optimize table test_bitmap_index final;

select 'int_vid';
set max_bytes_to_read = 5; -- expect to read only 5 bytes from bitmap index
select count() from test_bitmap_index where arraySetCheck(int_vid, 1); -- 1
select count() from test_bitmap_index where not arraySetCheck(int_vid, 1); -- 4
set max_bytes_to_read = 10; -- expect to read only 10 bytes from bitmap index (5 for each `arraySetCheck`)
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 4); -- 2
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 4); -- 0
select count() from test_bitmap_index where arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 4); -- 3
select count() from test_bitmap_index where arraySetCheck(int_vid, (1,2)) and arraySetCheck(int_vid, 4); -- 0
select count() from test_bitmap_index where arraySetCheck(int_vid, (1,4)) and arraySetCheck(int_vid, 4); -- 1
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)); -- 4
select count() from test_bitmap_index where not arraySetCheck(int_vid, 1) and not arraySetCheck(int_vid, 2); -- 3
set max_bytes_to_read = 15;
select count() from test_bitmap_index where (arraySetCheck(int_vid, (1,2)) or arraySetCheck(int_vid, 3)) and arraySetCheck(int_vid, 4); -- 0
set max_bytes_to_read = 25; -- column id + 5 bytes for bitmap index 
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and id = 1; -- 1
select count() from test_bitmap_index where not arraySetCheck(int_vid, 1) and id = 1; -- 0
set max_bytes_to_read = 0;
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, (1, 2)) and arraySetCheck(ext, 1) and id = 1; -- 1

select 'variadic';
set max_bytes_to_read = 15;
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3); -- 1
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) and arraySetCheck(int_vid, 3); -- 1
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and arraySetCheck(int_vid, 3); -- 0
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) settings enable_early_constant_folding = 0; -- 1 fix for enable_early_constant_folding enabled @wangtao
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)); -- 4
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) or arraySetCheck(int_vid, 2); -- 2
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2); -- 1
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(int_vid, 2); -- 1

select 'float_vid';
set max_bytes_to_read = 5;
select count() from test_bitmap_index where arraySetCheck(float_vid, 1); -- 1
select count() from test_bitmap_index where not arraySetCheck(float_vid, 1); -- 4
set max_bytes_to_read = 10;
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 4); -- 2
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index where arraySetCheck(float_vid, (1,2)) or arraySetCheck(float_vid, 4); -- 3
select count() from test_bitmap_index where arraySetCheck(float_vid, (1,2)) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index where arraySetCheck(float_vid, (1,4)) and arraySetCheck(float_vid, 4); -- 1
set max_bytes_to_read = 15;
select count() from test_bitmap_index where (arraySetCheck(float_vid, (1,2)) or arraySetCheck(float_vid, 3)) and arraySetCheck(float_vid, 4); -- 0
select count() from test_bitmap_index where not arraySetCheck(float_vid, 1) and not arraySetCheck(float_vid, 2); -- 3
select count() from test_bitmap_index where not (arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 2)); -- 4
select count() from test_bitmap_index where not ((arraySetCheck(float_vid, 1) or arraySetCheck(float_vid, 2)) and arraySetCheck(float_vid, 3)); -- 1
set max_bytes_to_read = 25; -- column id + 5 bytes for bitmap index 
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) and id = 1; -- 1
select count() from test_bitmap_index where not arraySetCheck(float_vid, 1) and id = 1; -- 0
set max_bytes_to_read = 0;
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1); -- 1
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) and arraySetCheck(ext, 1) and id = 1; -- 1
select count() from test_bitmap_index where arraySetCheck(float_vid, 1) and arraySetCheck(float_vid, (1, 2)) and arraySetCheck(ext, 1) and id = 1; -- 1

select 'variadic mixed';
set max_bytes_to_read = 15;
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) or arraySetCheck(int_vid, 2)) and arraySetCheck(float_vid, 3); -- 1
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) and arraySetCheck(float_vid, 3); -- 1
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2) and arraySetCheck(float_vid, 3); -- 0
select count() from test_bitmap_index where arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 1) settings enable_early_constant_folding = 0; -- 1 fix for enable_early_constant_folding enabled @wangtao
select count() from test_bitmap_index where not (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 1)); -- 4
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 1)) or arraySetCheck(int_vid, 2); -- 2
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(int_vid, 2)) or arraySetCheck(float_vid, 2); -- 1
select count() from test_bitmap_index where (arraySetCheck(int_vid, 1) and arraySetCheck(float_vid, 2)) or arraySetCheck(int_vid, 2); -- 1


select 'ad mocked';
set max_bytes_to_read = 25;
select id, multiIf(arraySetCheck(int_vid,1),'duck', arraySetCheck(int_vid,2),'boat', NULL) as name, count() from test_bitmap_index where arraySetCheck(int_vid, (1,2)) group by id, name order by id, name settings enable_optimizer=0; -- when enable_optimizer=1 this case is not stable
select id, multiIf(arraySetCheck(int_vid,1),'duck', arraySetCheck(int_vid,2),'boat', NULL) as name, count() from test_bitmap_index where arraySetCheck(int_vid, (1,2)) and id > 1 group by id, name order by id, name settings enable_optimizer=0;
set max_bytes_to_read = 0;
select * from test_bitmap_index where arraySetCheck(int_vid, (1,3,5)) order by id;

select 'mixed parts';

create table if not exists test_bitmap_index_mirror (date Date, id Int32, int_vid Array(Int32), float_vid Array(Float32), str_vid Array(String), ext Array(Int32)) engine = CnchMergeTree partition by date order by id settings min_bytes_for_wide_part = 0;

insert into test_bitmap_index_mirror values ('2019-01-03', 6, [6], [6], ['6'], [6]);
insert into test_bitmap_index_mirror values ('2019-01-03', 7, [7], [7], ['7'], [7]);

-- optimize table test_bitmap_index_mirror final;

alter table test_bitmap_index attach partition '2019-01-03' from test_bitmap_index_mirror;

select count() from test_bitmap_index where arraySetCheck(int_vid, (1,3,5,7));
select count() from test_bitmap_index where arraySetCheck(int_vid, (1,3,5,7)) and id > 1;
select id, multiIf(arraySetCheck(int_vid,3),'duck', arraySetCheck(int_vid,6),'boat', NULL) as name, count() from test_bitmap_index where arraySetCheck(int_vid, (3,6)) group by id, name order by id, name;


select 'bug';
create table array_test (`arr1` Array(Int64) BLOOM, `arr2` Array(Int64) BLOOM, p_date Date) engine = CnchMergeTree partition by tuple() order by tuple() SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;
insert into array_test select [1,2,3], [4,5,6], '2022-08-01' from numbers(100);
select count(1) from array_test prewhere p_date = '2022-08-01' where arraySetCheck(arr1, (1,2,3))  settings max_threads=1;

select 'join bug';
create table array_test2 (id UInt64, vids Array(Int64) BLOOM) engine = CnchMergeTree order by id SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;
insert into array_test2 values (1, [1]) (2, [2]);
select arraySetCheck(vids, 1) from array_test2 prewhere arraySetCheck(vids, 1) or arraySetCheck(vids, 2); -- do not add order by id, it will break the test
SELECT arraySetCheck(vids, 1)　FROM array_test2 AS et INNER JOIN　(　    SELECT toUInt32(number) AS id　    FROM numbers(10)　    LIMIT 2　) AS upt ON et.id = upt.id　WHERE arraySetCheck(vids, 1) OR arraySetCheck(vids, 2);

select 'map bug';
create table map_test (c1 UInt8, c2 Map(String, Array(String))) engine = CnchMergeTree order by c1;
insert into map_test(c1) values(1);
select * from map_test where arraySetCheck(c2{'k'}, ('v'));

drop table if exists test_bitmap_index_mirror;
drop table if exists test_bitmap_index;
drop table if exists array_test;
drop table if exists array_test2;
drop table if exists map_test;
