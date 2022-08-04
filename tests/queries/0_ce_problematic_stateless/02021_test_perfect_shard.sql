drop table if exists test.test_perfect_shard;
drop table if exists test.test_perfect_shard_local;

create table test.test_perfect_shard_local (p_date Date, id Int32, c1 Int32, c2 Int32) engine = MergeTree partition by p_date order by id;
create table test.test_perfect_shard as test.test_perfect_shard_local engine = Distributed(test_shard_localhost, test, test_perfect_shard_local);

insert into test.test_perfect_shard select '2021-01-01', number, number % 5, number % 10 from numbers(100);

set distributed_perfect_shard = 1;

select c1, count() from test.test_perfect_shard group by c1 order by c1;
select c1, sum(id) from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2) from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2), count() from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2), count() from test.test_perfect_shard group by c1 order by c1 limit 1;
select c1, sum(c2), count() from (select * from test.test_perfect_shard) group by c1 order by c1 limit 1;

select 'aliases';
select c1, count() as c from test.test_perfect_shard group by c1 order by c1;
select c1, sum(id) as s from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2) as s from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2) as s, count() as c from test.test_perfect_shard group by c1 order by c1;
select c1, sum(c2) as s, count() as c from test.test_perfect_shard group by c1 order by c1 limit 1;
select c1, sum(c2) as s, count() as c from (select * from test.test_perfect_shard) group by c1 order by c1 limit 1;
select c1, sum(c2) as s, count() as c from (select * from test.test_perfect_shard) group by c1 order by s limit 1;
select c1, sum(c2) as s, count() as c from (select * from test.test_perfect_shard) group by c1 order by s, c limit 1;
select c1, sum(c2) as s, count() as c from (select * from test.test_perfect_shard) group by c1 order by s, c;

select 'subquery';
select c1, sum(c2) as s, count() as c from (select c1, c2 from test.test_perfect_shard) group by c1 order by c1;
select c1, sum(c2) as s, count() as c from (select c1, c2, count() from test.test_perfect_shard group by c1, c2) group by c1 order by c1;
select c1, sum(c2) as s, count() as c from (select c1, c2 from test.test_perfect_shard group by c1, c2) group by c1 order by c1;

select 'countdistinct';
select countDistinct(c1) from test.test_perfect_shard;
select c1, countDistinct(id) from test.test_perfect_shard group by c1 order by c1;

select 'union all';
select c1, count() from test.test_perfect_shard group by c1 order by c1
union all
select c1, count() from test.test_perfect_shard group by c1 order by c1;

select 'having';
select c1, sum(id) from test.test_perfect_shard group by c1 having sum(id) > 1000 order by c1;
select c1, sum(id) as s from test.test_perfect_shard group by c1 having s > 1000 order by c1;

select 'constant';
select 1 as k, count() from test.test_perfect_shard group by k order by k;

drop table if exists test.test_perfect_shard;
drop table if exists test.test_perfect_shard_local;