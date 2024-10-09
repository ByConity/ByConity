drop table if exists test_partition_pushdown_5489234;

set enable_partition_filter_push_down = 1;
-- set enable_early_partition_pruning = 1;

create table test_partition_pushdown_5489234 (p_date Date, app_id Int32, id Int32) engine = CnchMergeTree
partition by (p_date, app_id) order by id;

insert into test_partition_pushdown_5489234 select '2023-01-01', 1, number from numbers(10);
insert into test_partition_pushdown_5489234 select '2023-01-02', 2, number from numbers(10);


select p_date, count() from test_partition_pushdown_5489234 where p_date = '2023-01-01' and app_id = 1 and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test_partition_pushdown_5489234 where toDate(p_date) = '2023-01-01' and app_id = 1 and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test_partition_pushdown_5489234 where (p_date = '2023-01-01' and app_id = 1) or id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test_partition_pushdown_5489234 where p_date = '2023-01-01' and (app_id = 1 or id % 2 = 1) group by p_date order by p_date;

select p_date, count() from test_partition_pushdown_5489234 where toDate(p_date) = '2023-01-01' or app_id = 2 or id % 2 = 1 group by p_date order by p_date;

drop table if exists test_partition_pushdown_5489234;