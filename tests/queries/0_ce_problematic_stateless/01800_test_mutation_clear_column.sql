set database_atomic_wait_for_drop_and_detach_synchronously = 1;
drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 19, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 20, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation order by p_date, id;

select '';
alter table test_mutation clear column age in partition id '20210101';
alter table test_mutation fastdelete age in partition '2021-01-02' where id = 2;
select sleep(1) format Null;
select 'clear column age in partition id 2021-01-01';
select 'fast delete age in partition 2021-01-02 where id = 2';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation clear column age in partition where p_date = '2021-01-05';
select sleep(1) format Null;
select 'test clear column where command which will not affect any part';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age where event = 'a';
alter table test_mutation clear column ab in partition where p_date = '2021-01-02' or p_date = '2021-01-03';
select sleep(1) format Null;
select 'fastdelete age where event = a';
select 'clear column ab in partition where p_date = 2021-01-02 or p_date = 2021-01-03';
select  * from test_mutation order by p_date, id;

drop table test_mutation;

--- test tuple partition
select '';
select '--------------------------------------------------------------------';
select 'start to test tuple partition';
CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32)) ENGINE = MergeTree PARTITION BY (p_date, id) ORDER BY id SETTINGS index_granularity = 8192;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 19, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 20, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 21, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 22, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-03', 3, 23, 'c', [1,2,3]);

select * from test_mutation order by p_date, id;

select '';
alter table test_mutation clear column age in partition id '20210101-1';
alter table test_mutation fastdelete age in partition ('2021-01-02', 2) where age = 21;
select sleep(1) format Null;
select 'clear column age in partition id 20210101-1';
select 'fast delete age in partition (2021-01-02, 2) where age = 21';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation clear column age in partition where p_date = '2021-01-05';
select sleep(1) format Null;
select 'test clear column where command which will not affect any part';
select  * from test_mutation order by p_date, id;

select '';
alter table test_mutation fastdelete age where event = 'a';
alter table test_mutation clear column ab in partition where p_date = '2021-01-02' or p_date = '2021-01-03';
select sleep(1) format Null;
select 'fastdelete age where event = a';
select 'clear column ab in partition where p_date = 2021-01-02 or p_date = 2021-01-03';
select  * from test_mutation order by p_date, id;

drop table test_mutation;
