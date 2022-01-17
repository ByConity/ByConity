drop table if exists test.test_subquery;
drop table if exists test.test_subquery_local;

create table test.test_subquery_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_subquery as test.test_subquery_local engine = Distributed(test_shard_localhost, test, test_subquery_local, rand());

set enable_distributed_stages = 1;

select id from test.test_subquery limit 10;
select id from test.test_subquery order by id limit 10;

insert into test.test_subquery_local select '2022-01-01', number, 'a' from numbers(3);

select id from (select * from test.test_subquery) order by id;
select * from (select id from test.test_subquery) order by id;

select id as a from (select * from test.test_subquery) order by a;
select * from (select id as a from test.test_subquery) order by a;

drop table if exists test.test_subquery;
drop table if exists test.test_subquery_local;