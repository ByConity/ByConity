set enable_optimizer = 1;
set optimize_skip_unused_shards = 1;
set enable_prune_source_plan_segment = 1;
set query_with_linear_table_version = 1;

drop table if exists bucket_prune;
create table bucket_prune (p Int32, i Int64, s String) engine = CnchMergeTree partition by p cluster by s into 4 buckets order by i;
insert into bucket_prune select 0, 0, toString(number) from numbers(100);
insert into bucket_prune select 0, 1, toString(number) from numbers(100);
insert into bucket_prune select 1, 0, toString(number) from numbers(100);
insert into bucket_prune select 1, 1, toString(number) from numbers(100);

select '1 bucket';
select * from bucket_prune where s = '10' and i = 1 order by p, i;
select '2 bucket';
select * from bucket_prune where s in ('10', '20') and p = 1 order by p, i, s;
select 'union all';
select t, p, i from (
    select 'a' as t, p, i from bucket_prune where s = '10' and i = 1
    union all
    select 'b' as t, p, i from bucket_prune where s = '20' and i = 1
) order by t, p, i;

drop table bucket_prune;

-- test bucket prune for catalog 2.0

drop table if exists bucket_prune2;
create table bucket_prune2 (p Int32, i Int64, s String) engine = CnchMergeTree partition by p cluster by s into 4 buckets order by i settings enable_publish_version_on_commit = 1;
select 'test empty table';
select * from bucket_prune2;

insert into bucket_prune2 select 0, 0, toString(number) from numbers(100);
insert into bucket_prune2 select 0, 1, toString(number) from numbers(100);
insert into bucket_prune2 select 1, 0, toString(number) from numbers(100);
insert into bucket_prune2 select 1, 1, toString(number) from numbers(100);

select '1 bucket';
select * from bucket_prune2 where s = '10' and i = 1 order by p, i;
select '2 bucket';
select * from bucket_prune2 where s in ('10', '20') and p = 1 order by p, i, s;
select 'union all';
select t, p, i from (
    select 'a' as t, p, i from bucket_prune2 where s = '10' and i = 1
    union all
    select 'b' as t, p, i from bucket_prune2 where s = '20' and i = 1
) order by t, p, i;

drop table bucket_prune2;
