drop table if exists tt1;
drop table if exists tt2;

create table tt1(a Int8) engine = CnchMergeTree() order by a;
create table tt2(b Int8) engine = CnchMergeTree() order by b;

insert into tt1 values (2)(3);
insert into tt2 values (3)(4);
-- { echoOn }

select count(*) from tt1, tt2 where a = b;
select count(*) from tt1, tt2 where toInt32(a) = toInt64(b);
select count(*) from tt1, tt2 where toInt32(a + 1) = toInt64(b + 1);

-- { echoOff }

drop table if exists tt1;
drop table if exists tt2;
