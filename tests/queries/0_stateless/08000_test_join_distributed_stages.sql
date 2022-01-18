drop table if exists test.test_join;
drop table if exists test.test_join_local;

create table test.test_join_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_join as test.test_join_local engine = Distributed(test_shard_localhost, test, test_join_local, rand());

set enable_distributed_stages = 1;
set exchange_enable_force_remote_mode = 1;

select id from test.test_join limit 10;
select id from test.test_join order by id limit 10;

insert into test.test_join_local select '2022-01-01', number, 'a' from numbers(3);

select id from test.test_join as a join test.test_join as b on a.id = b.id;
select a.id, b.id from test.test_join as a join test.test_join as b on a.id = b.id;
select a.id as i, b.id as j, i + j from test.test_join as a join test.test_join as b on a.id = b.id;
select * from test.test_join as a join test.test_join as b on a.id = b.id;
select * from test.test_join join test.test_join as b using id;

select id from test.test_join as a join test.test_join as b on a.id = b.id and a.event = b.event;
select id + 1 from test.test_join as a join test.test_join as b on a.id = b.id and a.event = b.event;
select toString(id + 1) from test.test_join as a join test.test_join as b on a.id = b.id and a.event = b.event;
select toString(id + 1) as i from test.test_join as a join test.test_join as b on a.id = b.id and a.event = b.event;
select * from test.test_join join test.test_join as b using (id, event);

select id from test.test_join as a left join test.test_join as b on a.id = b.id;
select id from test.test_join as a inner join test.test_join as b on a.id = b.id;
select id from test.test_join as a right join test.test_join as b on a.id = b.id;
select id from test.test_join as a full outer join test.test_join as b on a.id = b.id;

select id from test.test_join as a global left join test.test_join as b on a.id = b.id;
select id from test.test_join as a global inner join test.test_join as b on a.id = b.id;
select id from test.test_join as a global right join test.test_join as b on a.id = b.id;
select id from test.test_join as a global full outer join test.test_join as b on a.id = b.id;

drop table if exists test.test_join;
drop table if exists test.test_join_local;