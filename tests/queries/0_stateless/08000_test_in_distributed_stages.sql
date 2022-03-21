drop table if exists test.test_in;
drop table if exists test.test_in_local;

create table test.test_in_local (p_date Date, id Int32, event String) engine = MergeTree partition by p_date order by id;
create table test.test_in as test.test_in_local engine = Distributed(test_shard_localhost, test, test_in_local, rand());

set enable_distributed_stages = 1;
set exchange_enable_force_remote_mode = 1;
set send_plan_segment_by_brpc = 1;

insert into test.test_in_local select '2022-01-01', number, 'a' from numbers(3);

select * from test.test_in where id in 1;
select * from test.test_in where id in (1, 2) order by id;
select * from test.test_in where id in (1, 2, 3) order by id;

select * from test.test_in where id in 1 and event = 'a';
select * from test.test_in where id in (1, 2) and event = 'a' order by id;
select * from test.test_in where id in (1, 2, 3) and event = 'a' order by id;

select id from test.test_in where id in (1) or id in 2 order by id;

drop table if exists test.test_in;
drop table if exists test.test_in_local;