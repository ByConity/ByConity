drop table if exists test_subquery_join_local;

create table test_subquery_join_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;

select id from test_subquery_join_local limit 10;
select id from test_subquery_join_local order by id limit 10;

insert into test_subquery_join_local select '2022-01-01', number, 'a' from numbers(3);

select id from (select * from test_subquery_join_local) as a join (select * from test_subquery_join_local) as b on a.id = b.id order by id;
select id + 1 from (select * from test_subquery_join_local) as a join (select * from test_subquery_join_local) as b on a.id = b.id order by id;
select toString(id + 1) from (select * from test_subquery_join_local) as a join (select * from test_subquery_join_local) as b on a.id = b.id order by id;
select toString(id + 1) as i from (select * from test_subquery_join_local) as a join (select * from test_subquery_join_local) as b on a.id = b.id order by id;
select (id + 1) as i, i as j, j + 1 from (select * from test_subquery_join_local) as a join (select * from test_subquery_join_local) as b on a.id = b.id order by id;

select i, j from (select id as i from test_subquery_join_local) as a join (select id as j from test_subquery_join_local) as b on a.i = b.j order by i;
select i from (select id as i from test_subquery_join_local) as a join (select id as j from test_subquery_join_local) as b on a.i = b.j order by i;
select i from (select id + 1 as i from test_subquery_join_local) as a join (select id + 1 as j from test_subquery_join_local) as b on a.i = b.j order by i;
select * from (select id + 1 as i from test_subquery_join_local) as a join (select id + 1 as j from test_subquery_join_local) as b on a.i = b.j order by i;

drop table if exists test_subquery_join_local;