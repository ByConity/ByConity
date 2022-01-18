drop table if exists test.test_mutation;

CREATE TABLE test.test_mutation (`p_date` Date, `id` Int32, `age` Int32, `event` String COMPRESSION, ab Array(Int32) BLOOM) ENGINE = MergeTree PARTITION BY p_date ORDER BY id SETTINGS index_granularity = 8192, enable_build_ab_index = 1;

set enable_ab_index_optimization = 1;
set enable_dictionary_compression = 1;

insert into test.test_mutation values ('2021-01-01', 1, 18, 'a', [1,2,3]);
insert into test.test_mutation values ('2021-01-01', 2, 18, 'b', [1,2,3]);
insert into test.test_mutation values ('2021-01-01', 3, 18, 'c', [1,2,3]);

select * from test.test_mutation order by id;

alter table test.test_mutation fastdelete age where id = 1;

select sleep(1) format Null;

select  * from test.test_mutation order by id;

alter table test.test_mutation fastdelete id where event = 'c';

select sleep(1) format Null;

select  * from test.test_mutation order by id;

drop table test.test_mutation;
