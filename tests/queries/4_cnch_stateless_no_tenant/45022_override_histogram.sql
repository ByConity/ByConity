set create_stats_time_output=0;
create table orht_45022(x UInt64) ENGINE=CnchMergeTree() order by x;

insert into orht_45022 select * from system.numbers limit 10;

select 'no hist collect';
create stats orht_45022 settings statistics_collect_histogram=0;
show stats orht_45022;
show column_stats orht_45022;

select 'with hist collect';
create stats orht_45022 settings statistics_collect_histogram=1;
show stats orht_45022;
show column_stats orht_45022;

select 'no hist collect 2, should override';
create stats orht_45022 settings statistics_collect_histogram=0;
show stats orht_45022;
show column_stats orht_45022;

select 'drop';
drop stats orht_45022;
show stats orht_45022;
show column_stats orht_45022;

