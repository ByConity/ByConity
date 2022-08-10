drop table if exists test_mutation;
drop table if exists id_mutation;

CREATE TABLE test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String, `ab` Array(Int32)) 
ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

select * from test_mutation order by id;

alter table test_mutation fastdelete age where id = 1;

select * from test_mutation order by id;

alter table test_mutation fastdelete id where event = 'c';

select * from test_mutation order by id;

alter table test_mutation fastdelete id where event in (select event from test_mutation);

select * from test_mutation order by id;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

alter table test_mutation fastdelete id where event in ('a', 'b');

select * from test_mutation order by id;

optimize table test_mutation;

select * from test_mutation order by id;

CREATE TABLE id_mutation (`p_date` Date, `id` Int32) ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192;

insert into test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);
insert into test_mutation values ('2021-01-01', 4, 18, 'd', [1,2,3]);

insert into id_mutation values ('2021-01-01', 1), ('2021-01-01', 2);

alter table test_mutation fastdelete id where id in (select id from id_mutation);

select * from test_mutation order by id;

drop table id_mutation;
drop table test_mutation;
