use test;
drop table if exists tt1;
drop table if exists tt2;

create table tt1(a Int8) engine = CnchMergeTree() order by tuple();
create table tt2(b Int8) engine = CnchMergeTree() order by tuple();

insert into tt1 values (1);
insert into tt2 values (1);

-- { echoOn }

select count(*) from tt1, tt2 where a = b;
select count(*) from tt1, tt2 where toInt32(a) = toInt64(b);
select count(*) from tt1, tt2 where toInt32(a + 1) = toInt64(b + 1);

-- { echoOff }

drop table if exists tt1;
drop table if exists tt2;
