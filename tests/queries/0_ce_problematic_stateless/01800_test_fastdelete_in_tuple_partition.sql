drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String)
ENGINE = MergeTree PARTITION BY (p_date, id) ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a');
insert into test_mutation values ('2021-01-01', 1, 19, 'f');
insert into test_mutation values ('2021-01-01', 2, 20, 'b');
insert into test_mutation values ('2021-01-01', 2, 21, 'c');
insert into test_mutation values ('2021-01-02', 3, 22, 'a');
insert into test_mutation values ('2021-01-02', 3, 23, 'b');

select * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age in partition id '20210101-1' where age = 17;
select 'fast delete age in partition id 20210101-1 where age = 17';
select  * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age in partition id '2021-01-01-1' where age = 18;
select 'fast delete use wrong partition id';
select  * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age in partition id '20210101-1' where age = 18;
select 'fast delete age in partition id 20210101-1 where age = 18';
select  * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age where id = 2;
select 'fast delete age where id = 2';
select  * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age in partition ('2021-01-02',3) where event = 'b';
select 'fast delete age in partition (2021-01-02,3) where event = b';
select  * from test_mutation order by p_date, age;

select '';
alter table test_mutation fastdelete age where event = 'f';
select 'fast delete age where event = f';
select  * from test_mutation order by p_date, age;

drop table test_mutation;
