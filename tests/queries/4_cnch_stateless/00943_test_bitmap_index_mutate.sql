drop table if exists test_bitmap_index_mutate sync;

set enable_ab_index_optimization = 1;

create table if not exists test_bitmap_index_mutate (date Date, id Int32, int_vid Array(Int32) Bloom, float_vid Array(Float32) BitmapIndex, str_vid Array(String) BitmapIndex, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test_bitmap_index_mutate values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test_bitmap_index_mutate values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test_bitmap_index_mutate values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test_bitmap_index_mutate values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test_bitmap_index_mutate values ('2019-01-02', 5, [5], [5], ['5'], [5]);

select 'int_vid';
select count() from test_bitmap_index_mutate where arraySetCheck(int_vid, 1); -- 1

alter table test_bitmap_index_mutate add column k1 Int32;

select count() from test_bitmap_index_mutate where arraySetCheck(int_vid, 1); -- 1

drop table if exists test_bitmap_index_mutate sync;

set enable_ab_index_optimization = 1;

create table if not exists test_bitmap_index_mutate (date Date, id Int32, int_vid Array(Int32) BitmapIndex, float_vid Array(Float32) BitmapIndex, str_vid Array(String) BitmapIndex, ext Array(Int32)) 
engine = CnchMergeTree partition by date order by id settings enable_build_ab_index = 1;

insert into test_bitmap_index_mutate values ('2019-01-01', 1, [1], [1], ['1'], [1]);
insert into test_bitmap_index_mutate values ('2019-01-01', 2, [2], [2], ['2'], [2]);
insert into test_bitmap_index_mutate values ('2019-01-01', 3, [3], [3], ['3'], [3]);
insert into test_bitmap_index_mutate values ('2019-01-02', 4, [4], [4], ['4'], [4]);
insert into test_bitmap_index_mutate values ('2019-01-02', 5, [5], [5], ['5'], [5]);

select 'int_vid';
select count() from test_bitmap_index_mutate where arraySetCheck(int_vid, 1); -- 1

alter table test_bitmap_index_mutate add column k1 Int32;

select count() from test_bitmap_index_mutate where arraySetCheck(int_vid, 1); -- 1

drop table if exists test_bitmap_index_mutate sync;
