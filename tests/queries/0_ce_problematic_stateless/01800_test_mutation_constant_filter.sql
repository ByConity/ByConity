drop table if exists test_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, ab Array(Int32))
ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);

select * from test_mutation order by id;

alter table test_mutation fastdelete where 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'a', [1,2,3]);
alter table test_mutation fastdelete age where 1;

select  * from test_mutation order by id;

insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
alter table test_mutation fastdelete where 0;
alter table test_mutation fastdelete where 1 = 0;
alter table test_mutation fastdelete where 1 = 1;

select  * from test_mutation order by id;

alter table test_mutation fastdelete where id in (select toInt32(number * 20) from system.numbers limit 1000);

select count() from test_mutation;

drop table test_mutation;
