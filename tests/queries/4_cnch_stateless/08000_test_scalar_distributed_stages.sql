drop table if exists test_scalar2_local;

create table test_scalar2_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;

insert into test_scalar2_local select '2022-01-01', number, 'a' from numbers(3);

select 1;
select 'a';
select 1 + 1;
select * from test_scalar2_local where id = 1;
select * from test_scalar2_local where id = 1 and event = 'a';
select * from test_scalar2_local as a join test_scalar2_local as b on a.id = b.id order by id;

drop table if exists test_scalar2_local;