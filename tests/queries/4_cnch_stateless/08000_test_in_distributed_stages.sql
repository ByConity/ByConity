drop table if exists test_in_local;

create table test_in_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;
set exchange_enable_force_remote_mode = 1;

insert into test_in_local select '2022-01-01', number, 'a' from numbers(3);

select * from test_in_local where id in 1;
select * from test_in_local where id in (1, 2) order by id;
select * from test_in_local where id in (1, 2, 3) order by id;

select * from test_in_local where id in 1 and event = 'a';
select * from test_in_local where id in (1, 2) and event = 'a' order by id;
select * from test_in_local where id in (1, 2, 3) and event = 'a' order by id;

select id from test_in_local where id in (1) or id in 2 order by id;

select count() from test_in_local where event in 'a' or id in 2;
select count() from test_in_local where event in ('a', 'b') and id in 2;

drop table if exists test_in_local;