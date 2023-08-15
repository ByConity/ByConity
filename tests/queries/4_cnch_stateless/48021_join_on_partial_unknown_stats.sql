set create_stats_time_output=0;
drop table if exists join_on_unknown_stats1;
drop table if exists join_on_unknown_stats2;

create table if not exists join_on_unknown_stats1(a UInt64, b UInt64, c UInt64) Engine=CnchMergeTree order by a;

create table if not exists join_on_unknown_stats2(a UInt64, b UInt64, c UInt64) Engine=CnchMergeTree order by a;

insert into join_on_unknown_stats1 select number, number, number from system.numbers limit 10;
insert into join_on_unknown_stats2 select number, number, number from system.numbers limit 10;

create stats join_on_unknown_stats1(a, c);
create stats join_on_unknown_stats2(b, c);

select count(*) from join_on_unknown_stats1 as t1, join_on_unknown_stats2 as t2 where t1.a=t2.a and t1.b=t2.b and t1.c >= t2.c;

drop table if exists join_on_unknown_stats1;
drop table if exists join_on_unknown_stats2;
