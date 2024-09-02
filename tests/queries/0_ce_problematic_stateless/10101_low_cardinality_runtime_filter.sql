set enable_optimizer=1;
set create_stats_time_output=0;

drop table if exists lc_rf1_all;
drop table if exists lc_rf2_all;

create table lc_rf1_all (c1 UInt32, c2 LowCardinality(Nullable(UInt32))) engine = CnchMergeTree order by c1;
create table lc_rf2_all (c1 UInt32, c2 LowCardinality(Nullable(UInt32))) engine = CnchMergeTree order by c1;

insert into lc_rf1_all select 1, number from system.numbers limit 10000;
insert into lc_rf2_all select 1, number from system.numbers limit 10000;

create stats lc_rf1_all;
create stats lc_rf2_all;

select count(*) from lc_rf1_all a,lc_rf2_all b where a.c2=b.c2 and a.c1=1 settings enable_runtime_filter_cost=0, runtime_filter_min_filter_rows=0, runtime_filter_min_filter_factor=0;
select count(*) from lc_rf1_all a,lc_rf2_all b where a.c2=b.c2 and a.c1=1 settings enable_runtime_filter_cost=0, runtime_filter_min_filter_rows=0, runtime_filter_min_filter_factor=0, runtime_filter_bloom_build_threshold=0,runtime_filter_in_build_threshold=100000;

-- test tuple
select count(*) from lc_rf1_all a,lc_rf2_all b where (a.c2, a.c1)=(b.c2, b.c1) settings enable_runtime_filter_cost=0, runtime_filter_min_filter_rows=0, runtime_filter_min_filter_factor=0, runtime_filter_bloom_build_threshold=100000,runtime_filter_in_build_threshold=0;
select count(*) from lc_rf1_all a,lc_rf2_all b where (a.c2, a.c1)=(b.c2, b.c1) settings enable_runtime_filter_cost=0, runtime_filter_min_filter_rows=0, runtime_filter_min_filter_factor=0, runtime_filter_bloom_build_threshold=0,runtime_filter_in_build_threshold=100000;

drop table lc_rf1_all;
drop table lc_rf2_all;
