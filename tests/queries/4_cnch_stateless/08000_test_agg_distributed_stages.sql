drop table if exists test_agg_local;

create table test_agg_local (p_date Date, id Int32, event String) engine = CnchMergeTree partition by p_date order by id;

set enable_distributed_stages = 1;
set exchange_enable_force_remote_mode = 1;

select id from test_agg_local limit 10;
select id from test_agg_local order by id limit 10;

insert into test_agg_local select '2022-01-01', number, 'a' from numbers(3);

select count() from test_agg_local;
select sum(id) from test_agg_local;
select avg(id) from test_agg_local;
select avg(id) from test_agg_local as a join test_agg_local as b on a.id = b.id;
select sum(id) from (select * from test_agg_local) as a join (select * from test_agg_local) as b on a.id = b.id;
select sum(id) from (select id from test_agg_local) as a join (select id from test_agg_local) as b on a.id = b.id;
select sum(i) from (select id as i from test_agg_local) as a join (select id as j from test_agg_local) as b on a.i = b.j;

select sum(i) from
    (select id as i from test_agg_local) as a 
        join 
    (select id as j from test_agg_local) as b on a.i = b.j
        join
    (select id as k from test_agg_local) as c on a.i = c.k;

select sum(i) from
    (select id as i from test_agg_local) as a 
        join 
    (select id as j from test_agg_local) as b on a.i = b.j
        join
    (select id as k from test_agg_local) as c on a.i = c.k;

select sum(i) from
    (select max(id) as i from test_agg_local) as a 
        join 
    (select min(id) as j from test_agg_local) as b on a.i = b.j
        join
    (select any(id) as k from test_agg_local) as c on a.i = c.k;

insert into test_agg_local select '2022-01-01', number, 'b' from numbers(3);

select event, count() from test_agg_local group by event order by event;
select event, sum(id) from test_agg_local group by event order by event;
select event, avg(id) from test_agg_local group by event order by event;
select event, avg(id) from test_agg_local as a join test_agg_local as b on a.id = b.id group by event order by event;
select event, sum(id) from (select * from test_agg_local) as a join (select * from test_agg_local) as b on a.id = b.id group by event order by event;
select event, sum(id) from (select event, id from test_agg_local) as a join (select event, id from test_agg_local) as b on a.id = b.id group by event order by event;

drop table if exists test_agg_local;