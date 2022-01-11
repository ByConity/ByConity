drop table if exists test.test_join;
drop table if exists test.test_join_local;

create table test.test_join_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_join as test.test_join_local engine = Distributed(test_shard_localhost, test, test_join_local, rand());

set enable_distributed_stages = 1;

select id from test.test_join limit 10;
select id from test.test_join order by id limit 10;

insert into test.test_join_local select '2022-01-01', number, 'a' from numbers(3);

select id from (select * from test.test_join) as a join (select * from test.test_join) as b on a.id = b.id;
select id + 1 from (select * from test.test_join) as a join (select * from test.test_join) as b on a.id = b.id;
select toString(id + 1) from (select * from test.test_join) as a join (select * from test.test_join) as b on a.id = b.id;
select toString(id + 1) as i from (select * from test.test_join) as a join (select * from test.test_join) as b on a.id = b.id;
select (id + 1) as i, i as j, j + 1 from (select * from test.test_join) as a join (select * from test.test_join) as b on a.id = b.id;

select i, j from (select id as i from test.test_join) as a join (select id as j from test.test_join) as b on a.i = b.j;
select i from (select id as i from test.test_join) as a join (select id as j from test.test_join) as b on a.i = b.j;
select i from (select id + 1 as i from test.test_join) as a join (select id + 1 as j from test.test_join) as b on a.i = b.j;
select * from (select id + 1 as i from test.test_join) as a join (select id + 1 as j from test.test_join) as b on a.i = b.j;

drop table if exists test.test_join;
drop table if exists test.test_join_local;