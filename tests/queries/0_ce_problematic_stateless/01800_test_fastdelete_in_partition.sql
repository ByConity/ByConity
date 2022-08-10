drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32))
ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 19, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 20, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age in partition '2021-01-03' where id = 2;
select 'fast delete age in partition 2021-01-03 where id = 2';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age in partition id '2021-01-03' where id = 2;
select 'fast delete wrong usage: use partition name as partition id';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age in partition id '20210101' where id = 2;
select 'fast delete age in partition id 20210101 where id = 2';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age where id = 2;
select 'fast delete age where id = 2';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete id in partition '2021-01-01' where event = 'c';
select 'fast delete id in partition 2021-01-01 where event = c';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete id where event = 'c';
select 'fast delete id where event = c';
select  * from test_mutation order by p_date, id;

drop table test_mutation;
