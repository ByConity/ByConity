drop table if exists test.test_partition_pushdown_5489234;
drop table if exists test.test_partition_pushdown_5489234_local;

set enable_partition_filter_push_down = 1;
set enable_early_partition_pruning = 1;

create table test.test_partition_pushdown_5489234_local (p_date Date, app_id Int32, id Int32) engine = MergeTree
partition by (p_date, app_id) order by id;

insert into test.test_partition_pushdown_5489234_local select '2023-01-01', 1, number from numbers(10);
insert into test.test_partition_pushdown_5489234_local select '2023-01-02', 2, number from numbers(10);

create table test.test_partition_pushdown_5489234 as test.test_partition_pushdown_5489234_local
engine = Distributed(test_shard_localhost, test, test_partition_pushdown_5489234_local);

select p_date, count() from test.test_partition_pushdown_5489234 where p_date = '2023-01-01' and app_id = 1 and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test.test_partition_pushdown_5489234 where toDate(p_date) = '2023-01-01' and app_id = 1 and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test.test_partition_pushdown_5489234 where (p_date = '2023-01-01' and app_id = 1) or id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test.test_partition_pushdown_5489234 where p_date = '2023-01-01' and (app_id = 1 or id % 2 = 1) group by p_date order by p_date;

select p_date, count() from test.test_partition_pushdown_5489234 where toDate(p_date) = '2023-01-01' or app_id = 2 or id % 2 = 1 group by p_date order by p_date;

drop table if exists test.test_partition_pushdown_5489234;
drop table if exists test.test_partition_pushdown_5489234_local;