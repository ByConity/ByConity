set create_stats_time_output=0;
drop database if exists `test_45010`;
create database test_45010;
use test_45010;

create table a (x UInt8) ENGINE = CnchMergeTree() order by x;
create table ba (x UInt8) ENGINE = CnchMergeTree() order by x;
create table aa (x UInt8) ENGINE = CnchMergeTree() order by x;
create table aaa (x UInt8) ENGINE = CnchMergeTree() order by x;
insert into a values(1);
insert into aa values(1)(2);
insert into aaa values(1)(2)(3);

-- due to materialized view bug, we have to do it manually 
insert into ba select x from a;
create view v as select * from a;
create materialized view mv to ba as select x from a;

create stats all;
show stats all;
drop stats all;
set enable_optimizer=1;
set enable_materialized_view_rewrite=0;
explain select * from v;
explain select * from mv;
explain select * from a;
create stats all settings statistics_exclude_tables_regex='a.*';
explain select * from v;
explain select * from mv;
explain select * from a;
create stats all settings statistics_exclude_tables_regex='b.*';
explain select * from v;
explain select * from mv;
explain select * from a;

-- bug of alias at mv rewrite rule
select ali.x from mv as ali inner join system.one on dummy=ali.x;

drop stats all;
drop database test_45010;
