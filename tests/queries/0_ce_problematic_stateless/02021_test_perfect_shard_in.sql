drop table if exists test.test_perfect_shard;
drop table if exists test.test_perfect_shard_local;

create table test.test_perfect_shard_local (p_date Date, id Int32, c1 Int32, c2 Int32) engine = MergeTree partition by p_date order by id;
create table test.test_perfect_shard as test.test_perfect_shard_local engine = Distributed(test_shard_localhost, test, test_perfect_shard_local);

insert into test.test_perfect_shard select '2021-01-01', number, number % 5, number % 10 from numbers(100);

set distributed_perfect_shard = 1;

select c1, count() from 
    test.test_perfect_shard where id in (select id from test.test_perfect_shard) group by c1 order by c1;

select c1, count() from 
    test.test_perfect_shard where id in (select id from test.test_perfect_shard group by id) group by c1 order by c1;

select c1, count() from 
    test.test_perfect_shard where id global in (select id from test.test_perfect_shard group by id) group by c1 order by c1;

drop table if exists test.test_perfect_shard;
drop table if exists test.test_perfect_shard_local;
