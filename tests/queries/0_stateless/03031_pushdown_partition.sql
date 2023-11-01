drop table if exists test.test_partition_pushdown_5489;
drop table if exists test.test_partition_pushdown_5489_local;

set enable_partition_filter_push_down = 1;
set enable_early_partition_pruning = 1;

create table test.test_partition_pushdown_5489_local (p_date Date, id Int32) engine = MergeTree
partition by p_date order by id;

insert into test.test_partition_pushdown_5489_local select '2023-01-01', number from numbers(10);
insert into test.test_partition_pushdown_5489_local select '2023-01-02', number from numbers(10);

create table test.test_partition_pushdown_5489 as test.test_partition_pushdown_5489_local
engine = Distributed(test_shard_localhost, test, test_partition_pushdown_5489_local);

select p_date, count() from test.test_partition_pushdown_5489 where p_date = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test.test_partition_pushdown_5489_local where p_date = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test.test_partition_pushdown_5489 where toDate(p_date) = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test.test_partition_pushdown_5489_local where toDate(p_date) = '2023-01-01' and id % 2 = 1 group by p_date order by p_date;


select p_date, count() from test.test_partition_pushdown_5489 where p_date = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test.test_partition_pushdown_5489_local where p_date = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;

select p_date, count() from test.test_partition_pushdown_5489 where toDate(p_date) = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;
select p_date, count() from test.test_partition_pushdown_5489_local where toDate(p_date) = '2023-01-01' or id % 2 = 1 group by p_date order by p_date;

drop table if exists test.test_partition_pushdown_5489;
drop table if exists test.test_partition_pushdown_5489_local;