-- test catalog_enable_multiple_threads

drop table if exists t10085;
create table if not exists t10085 (a Int32, b Int32) engine = CnchMergeTree order by a;

-- write 9 parts
system stop merges t10085;
insert into t10085 values (1, 10);
insert into t10085 values (2, 20);
insert into t10085 values (3, 30);
insert into t10085 values (4, 40);
insert into t10085 values (5, 50);
insert into t10085 values (6, 60);
insert into t10085 values (7, 70);
insert into t10085 values (8, 80);
insert into t10085 values (9, 90);

select 'test wo/ catalog_enable_multiple_threads';
select * from t10085 order by a settings catalog_enable_multiple_threads = 0, max_threads=4;
select 'test w/ catalog_enable_multiple_threads';
select * from t10085 order by a settings catalog_enable_multiple_threads = 1, max_threads=4;
drop table if exists t10085;
