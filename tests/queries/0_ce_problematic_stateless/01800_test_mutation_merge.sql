drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32))
ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

select * from test_mutation order by p_date, id;

alter table test_mutation fastdelete id, age where id = 1;

optimize table test_mutation;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);

alter table test_mutation fastdelete id, age where id = 1 and p_date = '2021-01-02';

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

optimize table test_mutation;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);

optimize table test_mutation;

select sleep(1) format Null;

-- wait for previous mtuations to finish
alter table test_mutation fastdelete where id > 100 settings mutations_sync = 1;
select  * from test_mutation order by p_date, id;

optimize table test_mutation;

alter table test_mutation fastdelete id, event where event = 'c';
alter table test_mutation fastdelete id, event, ab where event = 'd';

optimize table test_mutation;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

optimize table test_mutation;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);

-- wait for previous mtuations to finish
alter table test_mutation fastdelete where id > 100 settings mutations_sync = 1;
select  * from test_mutation order by p_date, id;

alter table test_mutation fastdelete id, event, ab where event in ('a', 'b');
alter table test_mutation fastdelete id, event, ab where event in ('b', 'c');

-- wait for previous mtuations to finish
alter table test_mutation fastdelete where id > 100 settings mutations_sync = 1;
select  * from test_mutation order by p_date, id;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);

optimize table test_mutation;

select sleep(1) format Null;

select  * from test_mutation order by p_date, id;

drop table test_mutation;
