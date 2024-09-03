drop table if exists date_dim;
drop table if exists date_dim_local;
set dialect_type='ANSI';
create table date_dim (date Date, year Int64, month Int64) engine=CnchMergeTree() order by tuple();

-- 2 year data
insert into date_dim select toDate('2021-01-01') + number as date, toYear(date), toMonth(date) from system.numbers limit 730;

select 'how many days in each year/month';
select year, month, count() from date_dim group by grouping sets (year, month) order by (year, month);
select 'how many days in Jan./Feb./Mar.';
select year, month, count() from date_dim group by grouping sets (year, month) having month in (1, 2, 3) order by (year, month);
select 'how many days in Jan./Feb./Mar., and each year';
select year, month, count() from date_dim group by grouping sets (year, month) having month in (1, 2, 3) or (month is Null) order by (year, month);

drop table if exists date_dim;
drop table if exists date_dim_local;