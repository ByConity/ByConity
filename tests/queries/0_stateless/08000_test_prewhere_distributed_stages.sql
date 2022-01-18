drop table if exists test.test_prewhere;
drop table if exists test.test_prewhere_local;

create table test.test_prewhere_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_prewhere as test.test_prewhere_local engine = Distributed(test_shard_localhost, test, test_prewhere_local, rand());

set enable_distributed_stages = 0;

insert into test.test_prewhere_local select '2022-01-01', number, 'a' from numbers(3);

select * from test.test_prewhere where id in 1 and event = 'a';
select * from test.test_prewhere where id in (1, 2) and event = 'a' order by id;
select * from test.test_prewhere where id in (1, 2, 3) and event = 'a' order by id;
select * from test.test_prewhere where id = 1 and event = 'a' order by id;

select id from test.test_prewhere prewhere id = 1;
select id from test.test_prewhere prewhere id = 1 and event = 'a';
select id from test.test_prewhere prewhere id = 1 or event = 'a' order by id;

select id from test.test_prewhere prewhere id = 1 where id = 1;
select id from test.test_prewhere prewhere id = 1 and event = 'a' where id = 1 and event = 'a';
select id from test.test_prewhere prewhere id = 1 or event = 'a' where id = 1 or event = 'a' order by id;

select id from test.test_prewhere prewhere id = 1 where id = 1 and id = 1;
select id from test.test_prewhere prewhere id = 1 and event = 'a' where id = 1 and event = 'a' and id = 1 and event = 'a';
select id from test.test_prewhere prewhere id = 1 or event = 'a' where id = 1 or event = 'a' or id = 1 or event = 'a' order by id;

drop table if exists test.test_prewhere;
drop table if exists test.test_prewhere_local;