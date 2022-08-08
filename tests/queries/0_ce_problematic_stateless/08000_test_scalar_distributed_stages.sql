drop table if exists test.test_scalar2;
drop table if exists test.test_scalar2_local;

create table test.test_scalar2_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_scalar2 as test.test_scalar2_local engine = Distributed(test_shard_localhost, test, test_scalar_local, rand());

set enable_distributed_stages = 1;

insert into test.test_scalar2_local select '2022-01-01', number, 'a' from numbers(3);

select 1;
select 'a';
select 1 + 1;
select * from test.test_scalar2_local where id = 1;
select * from test.test_scalar2_local where id = 1 and event = 'a';
select * from test.test_scalar2_local as a join test.test_scalar2_local as b on a.id = b.id order by id;

drop table if exists test.test_scalar2;
drop table if exists test.test_scalar2_local;