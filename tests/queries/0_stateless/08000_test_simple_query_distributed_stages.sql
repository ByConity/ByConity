drop table if exists test.test_simple;
drop table if exists test.test_simple_local;

create table test.test_simple_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_simple as test.test_simple_local engine = Distributed(test_shard_localhost, test, test_simple_local, rand());

set enable_distributed_stages = 1;

select id from test.test_simple limit 10;
select id from test.test_simple order by id limit 10;

insert into test.test_simple_local select '2022-01-01', number, 'a' from numbers(10);

select id from test.test_simple limit 10;
select id from test.test_simple order by id limit 10;

select * from test.test_simple;

drop table if exists test.test_simple;
drop table if exists test.test_simple_local;