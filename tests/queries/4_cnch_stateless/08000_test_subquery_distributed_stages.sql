drop table if exists test_subquery_local;

create table test_subquery_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;
set exchange_enable_force_remote_mode = 1;

select id from test_subquery_local limit 10;
select id from test_subquery_local order by id limit 10;

insert into test_subquery_local select '2022-01-01', number, 'a' from numbers(3);

select id from (select * from test_subquery_local) order by id;
select * from (select id from test_subquery_local) order by id;

select id as a from (select * from test_subquery_local) order by a;
select * from (select id as a from test_subquery_local) order by a;

drop table if exists test_subquery_local;