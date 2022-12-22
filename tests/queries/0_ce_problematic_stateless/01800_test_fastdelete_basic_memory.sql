drop table if exists test_fastdelete;

create table test_fastdelete (d Date, c1 Int64, c2 String, c3 Nullable(Int64), c4 Array(Int32))
engine=MergeTree partition by d order by c1 SETTINGS min_rows_for_compact_part = 100;

set mutations_sync = 1;

select 'test fastdelete on empty table';
alter table test_fastdelete fastdelete where c1 = 0;
select count(1) from test_fastdelete;

select 'insert rows';
insert into test_fastdelete select '2021-01-01', number, toString(number), if(number % 2, number, Null), array(number, number) from system.numbers limit 10;
select * from test_fastdelete order by c1;

-- delete 0 rows
select 'fastdelete where c2=a';
alter table test_fastdelete fastdelete where c2='a';
select * from test_fastdelete order by c1;

select 'fastdelete c1 where c1 between 0 and 1';
alter table test_fastdelete fastdelete c1 where c1 between 0 and 1;
select * from test_fastdelete order by c1;

select 'fastdelete c1, c2 where c2 in (1, 2)';
alter table test_fastdelete fastdelete c1, c2 where c2 in ('1', '2');
select * from test_fastdelete order by c1;

select 'fastdelete c3 where c3 between 2 and 3';
alter table test_fastdelete fastdelete c3 where c3 between 2 and 3;
select * from test_fastdelete order by c1;

select 'fastdelete c4 where c1 <= 4';
alter table test_fastdelete fastdelete c4 where c1 <= 4;
select * from test_fastdelete order by c1;

select 'fastdelete d where c1 = 5';
alter table test_fastdelete fastdelete d where c1 = 5;
select * from test_fastdelete order by c1;

-- delete 0 column and 2 row
select 'fastdelete where c1=7 or c2=8';
alter table test_fastdelete fastdelete where c1=7 or c2='8';
select * from test_fastdelete order by c1;

-- delete all columns and 1 row
select 'fastdelete d, c1, c2, c3, c4 where c1 = 9';
alter table test_fastdelete fastdelete d, c1, c2, c3, c4 where c1 = 9;
select * from test_fastdelete order by c1;

-- delete all rows
select 'fastdelete all rows';
alter table test_fastdelete fastdelete where c1 in (select toInt64(c2) from test_fastdelete);
select * from test_fastdelete order by c1;
select count(1) from test_fastdelete;

-- delete on empty part
select 'fastdelete where c1<100';
alter table test_fastdelete fastdelete where c1 < 100;
select * from test_fastdelete order by c1;
select count(1) from test_fastdelete;

drop table test_fastdelete;

