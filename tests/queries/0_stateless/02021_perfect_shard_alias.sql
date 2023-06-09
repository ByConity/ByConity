drop table if exists test.test_perfect_shard_alias;
drop table if exists test.test_perfect_shard_alias_local;

create table test.test_perfect_shard_alias_local (p_date Date, id Int32, c1 Int32, c2 Int32) engine = MergeTree partition by p_date order by id;
create table test.test_perfect_shard_alias as test.test_perfect_shard_alias_local engine = Distributed(test_shard_localhost, test, test_perfect_shard_alias_local);

insert into test.test_perfect_shard_alias select '2023-01-01', number, number % 5, number % 10 from numbers(100);

set distributed_perfect_shard = 1;

select p_date, count() from test.test_perfect_shard_alias group by p_date;
select p_date, count() as cnt from test.test_perfect_shard_alias group by p_date;
select p_date, count(id) as cnt from test.test_perfect_shard_alias group by p_date;

drop table if exists test.test_perfect_shard_alias;
drop table if exists test.test_perfect_shard_alias_local;