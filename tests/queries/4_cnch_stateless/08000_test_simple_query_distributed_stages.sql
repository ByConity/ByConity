drop table if exists test_simple_local;

create table test_simple_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;

select id from test_simple_local limit 10;
select id from test_simple_local order by id limit 10;

insert into test_simple_local select '2022-01-01', number, 'a' from numbers(10);

select id from test_simple_local limit 10;
select id from test_simple_local order by id limit 10;

select * from test_simple_local;

drop table if exists test_simple_local;