drop table if exists push_partial_agg;
drop table if exists push_partial_agg_all;

create table push_partial_agg (c1 decimal(28,4), c2 decimal (38,4)) engine=MergeTree() order by tuple();
create table push_partial_agg_all (c1 decimal(28,4), c2 decimal (38,4)) engine=Distributed(test_shard_localhost, currentDatabase(), push_partial_agg);

select sum(c1) from (select c1 from push_partial_agg_all union all select c2 as c1 from push_partial_agg_all);

drop table if exists push_partial_agg;
drop table if exists push_partial_agg_all;
