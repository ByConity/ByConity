drop table if exists test_in_detail_10723_local;
drop table if exists test_in_bitmap_local_10723;

create table test_in_detail_10723_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;
create table test_in_bitmap_local_10723 (p_date Date, uids BitMap64, event String) engine = CnchMergeTree partition by p_date order by event;

SET enable_optimizer = 0;

insert into test_in_detail_10723_local select '2022-01-01', number + 1, 'a' from numbers(3);

select '=============';
select * from test_in_detail_10723_local where id in (select arrayToBitmap(emptyArrayUInt32())) order by id, event;
select * from test_in_detail_10723_local where id in (select arrayToBitmap([1]));
select * from test_in_detail_10723_local where id in (select arrayToBitmap([1,2]));
select * from test_in_detail_10723_local where id in (select arrayToBitmap([1,2,3]));

insert into test_in_bitmap_local_10723 values ('2022-01-01', [], 'a');
insert into test_in_bitmap_local_10723 values ('2022-01-01', [1], 'b');
insert into test_in_bitmap_local_10723 values ('2022-01-01', [1,2], 'c');
insert into test_in_bitmap_local_10723 values ('2022-01-01', [1,2,3], 'd');

select '=============';
select * from test_in_detail_10723_local where id in (select uids from test_in_bitmap_local_10723 where event = 'a') order by id, event;
select * from test_in_detail_10723_local where id not in (select uids from test_in_bitmap_local_10723 where event = 'a') order by id, event;
select * from test_in_detail_10723_local where id in (select uids from test_in_bitmap_local_10723 where event = 'b') order by id, event;
select * from test_in_detail_10723_local where id not in (select uids from test_in_bitmap_local_10723 where event = 'b') order by id, event;
select * from test_in_detail_10723_local where id in (select uids from test_in_bitmap_local_10723 where event = 'c') order by id, event;
select * from test_in_detail_10723_local where id not in (select uids from test_in_bitmap_local_10723 where event = 'c') order by id, event;
select * from test_in_detail_10723_local where id in (select uids from test_in_bitmap_local_10723 where event = 'd') order by id, event;
select * from test_in_detail_10723_local where id not in (select uids from test_in_bitmap_local_10723 where event = 'd') order by id, event;
select '=============';
select * from test_in_detail_10723_local where id in (select uids from test_in_bitmap_local_10723 where event IN ('b','c')) order by id, event;
select * from test_in_detail_10723_local where id in (select bitmapExtract('1|2')(multiIf(event = 'b', 1, event = 'c', 2, -100) as idx, uids) from test_in_bitmap_local_10723 where event IN ('b','c')) order by id, event;

drop table if exists test_in_detail_10723_local;
drop table if exists test_in_bitmap_local_10723;

drop table if exists test_id_10723 sync;
drop table if exists test_id_map_10723 sync;
create table test_id_10723 (id UInt64) engine=CnchMergeTree order by id;
insert into test_id_10723 select number from numbers(10);
create table test_id_map_10723 (a Int32, uids BitMap64)engine=CnchMergeTree order by a;
insert into test_id_map_10723 select number as a, bitmapFromColumn(number) as uids from (select number from numbers(10)) group by a,number;

select '------------';
select * from test_id_10723 where id IN (select uids from test_id_map_10723 where a < 5) order by id;
select * from test_id_10723 where id NOT IN (select uids from test_id_map_10723 where a < 5) order by id;