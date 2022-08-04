drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32))
ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);
insert into test_mutation values ('2021-01-01', 5, 18, 'e', [1,2,3]);

select * from test_mutation order by id;

alter table test_mutation fastdelete age where id in (1, 2);
alter table test_mutation fastdelete age where id in (2, 3);

select  * from test_mutation order by id;

alter table test_mutation fastdelete age where id <= 4;

select  * from test_mutation order by id;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);
insert into test_mutation values ('2021-01-02', 5, 18, 'e', [1,2,3]);

alter table test_mutation fastdelete age where id in (1, 2) and p_date = '2021-01-01';
alter table test_mutation fastdelete age where id in (2, 3) and p_date = '2021-01-01';

select  * from test_mutation order by p_date, id;

alter table test_mutation fastdelete age where id < 3 and p_date = '2021-01-02';
alter table test_mutation fastdelete age where id < 5 and p_date = '2021-01-02';

select  * from test_mutation order by p_date, id;

insert into test_mutation values ('2021-01-02', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-02', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-02', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-02', 4, 18, 'd', [1,2,3]);
insert into test_mutation values ('2021-01-02', 5, 18, 'e', [1,2,3]);

optimize table test_mutation;

select  * from test_mutation order by p_date, id;

drop table test_mutation;
