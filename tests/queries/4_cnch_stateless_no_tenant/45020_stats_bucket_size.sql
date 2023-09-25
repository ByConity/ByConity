drop database if exists db_45020_bucket_size;
create database db_45020_bucket_size; 
use db_45020_bucket_size;
set create_stats_time_output=0;

create table t(x UInt64) ENGINE=CnchMergeTree() order by x;

insert into t select * from system.numbers limit 10;

set statistics_histogram_bucket_size=2;
create stats t;
show stats t;
show column_stats t;
drop stats t;

set statistics_histogram_bucket_size=1;
create stats t;
show stats t;
show column_stats t;
drop stats t;

set statistics_histogram_bucket_size=0;
create stats t;
show stats t;
show column_stats t;
drop stats t;

drop database db_45020_bucket_size;