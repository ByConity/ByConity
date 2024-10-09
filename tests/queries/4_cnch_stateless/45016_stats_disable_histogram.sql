set create_stats_time_output=0;
drop table if exists stats_disable_hist;
create table stats_disable_hist(x UInt64, y Float64) Engine=CnchMergeTree() order by x;

insert into stats_disable_hist select number, number from system.numbers limit 3;

set statistics_enable_sample=0;

select 'sample=0 & histogram=1';
drop stats stats_disable_hist;
set statistics_collect_histogram=1;
create stats stats_disable_hist;
show stats stats_disable_hist;
show column_stats stats_disable_hist;

select 'sample=0 & histogram=0';
drop stats stats_disable_hist;
set statistics_collect_histogram=0;
create stats stats_disable_hist;
show stats stats_disable_hist;
show column_stats stats_disable_hist;

set statistics_enable_sample=1;

select 'sample=1 & histogram=1';
drop stats stats_disable_hist;
set statistics_collect_histogram=1;
create stats stats_disable_hist;
show stats stats_disable_hist;
show column_stats stats_disable_hist;

select 'sample=1 & histogram=0';
drop stats stats_disable_hist;
set statistics_collect_histogram=0;
create stats stats_disable_hist;
show stats stats_disable_hist;
show column_stats stats_disable_hist;

drop stats stats_disable_hist;

drop table if exists stats_disable_hist;