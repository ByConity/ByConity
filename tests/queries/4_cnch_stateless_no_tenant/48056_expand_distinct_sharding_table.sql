drop table if exists tt;
create table tt (a UInt64, b UInt64, c UInt64) Engine = CnchMergeTree 
cluster by a into 16 buckets
order by b;

insert into tt select intDiv(number, 32 * 32), intDiv(number, 32) % 32, number % 32 from system.numbers limit 32768;

set enable_expand_distinct_optimization=1;
select count() from tt;
select uniqExact(a) from tt;
select uniqExact(b) from tt;
select uniqExact(c) from tt;
select count(), uniqExact(a), uniqExact(b), uniqExact(c) from tt;

drop table if exists tt;