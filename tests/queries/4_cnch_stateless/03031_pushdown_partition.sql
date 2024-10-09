drop table if exists test_partition_pushdown_5489;

set enable_partition_filter_push_down = 1;
-- set enable_early_partition_pruning = 1;

create table test_partition_pushdown_5489 (p_date Date, id Int32) engine = CnchMergeTree
partition by p_date order by id;

insert into test_partition_pushdown_5489 select '2023-01-01', number from numbers(10);
insert into test_partition_pushdown_5489 select '2023-01-02', number from numbers(10);

select p_date, count() from test_partition_pushdown_5489 where p_date = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test_partition_pushdown_5489 where toDate(p_date) = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;


select p_date, count() from test_partition_pushdown_5489 where p_date = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test_partition_pushdown_5489 where toDate(p_date) = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;

drop table if exists test_partition_pushdown_5489;