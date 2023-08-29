use test;
drop table if exists test1;
set optimize_trivial_count_query = 1;
set enable_optimizer_white_list = 0;
set enable_optimizer = 1;
create table test1(p DateTime, i int, j int) ENGINE = CnchMergeTree() partition by (toDate(p), i) order by j;
insert into test1 values ('2020-09-01 00:01:02', 1, 2), ('2020-09-01 00:01:03', 2, 3), ('2020-09-02 00:01:03', 3, 4);

select count() from test1;
select count() from test1 where toDate(p) > '2020-09-01';
select count() from test1 prewhere i = 1 where toDate(p) > '2020-09-01';
select count() from test1 prewhere i > 1;
select count() from test1 where i < 1;
select * from (select count() as cnt from test1 where i > 1) t where t.cnt < 10;
select count() from (select i iiii from test1) where iiii < 1 union all select count() from (select i iiii from test1) where iiii < 1;
select count() from (select i iiii from test1 prewhere iiii < 10 where i < 10) where iiii>1;
select count() from (select i iiii from test1 prewhere iiii + i < 10 where j + iiii < 10) where iiii > 1;
select count() from test1 where i+i < 1;
select i,count() from test1 group by i order by i;
select count() from test1 t1 left join test1 t2 on t1.i=t2.i;

set enable_optimizer = 1;
-- optimized
select 'optimized:';
explain select count() from test1;

-- optimized
select 'optimized:';
explain select count() from test1 where toDate(p) > '2020-09-01';

-- optimized
select 'optimized:';
explain select count() from test1 prewhere i = 1 where toDate(p) > '2020-09-01';

-- optimized
select 'optimized:';
explain select count() from test1 prewhere i > 1;

-- optimized
select 'optimized:';

explain select count() from test1 where i < 1;

-- optimized
select 'optimized:';

explain select * from (select count() as cnt from test1 where i > 1) t where t.cnt < 10;

-- optimized
select 'optimized:';
explain select count() from (select i iiii from test1) where iiii < 1 union all select count() from (select i iiii from test1) where iiii < 1;

-- optimized
select 'optimized:';
explain select count() from (select i iiii from test1 prewhere iiii < 10 where i < 10) where iiii>1;

-- non-optimized
select 'non-optimized:';
explain select count() from (select i iiii from test1 prewhere iiii + i < 10 where j + iiii < 10) where iiii > 1;

-- non-optimized
select 'non-optimized:';
explain select count() from test1 where i+i < 1;

-- non-optimized
select 'non-optimized:';
explain select i,count() from test1 group by i;

-- non-optimized
select 'non-optimized:';
explain select count() from test1 t1 left join test1 t2 on t1.i=t2.i;

drop table if exists test1;
