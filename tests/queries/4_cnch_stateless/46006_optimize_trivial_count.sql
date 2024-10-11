drop table if exists test46006;
set optimize_trivial_count_query = 1;
set enable_optimizer = 1;
create table test46006(p DateTime, i int, j int) ENGINE = CnchMergeTree() partition by (toDate(p), i) order by j;
insert into test46006 values ('2020-09-01 00:01:02', 1, 2), ('2020-09-01 00:01:03', 2, 3), ('2020-09-02 00:01:03', 3, 4);

select count() from test46006;
select count() from test46006 where toDate(p) > '2020-09-01';
select count() from test46006 prewhere i = 1 where toDate(p) > '2020-09-01';
select count() from test46006 prewhere i > 1;
select count() from test46006 where i < 1;
select * from (select count() as cnt from test46006 where i > 1) t where t.cnt < 10;
select count() from (select i iiii from test46006) where iiii < 1 union all select count() from (select i iiii from test46006) where iiii < 1;
select count() from (select i iiii from test46006 prewhere iiii < 10 where i < 10) where iiii>1;
select count() from (select i iiii from test46006 prewhere iiii + i < 10 where j + iiii < 10) where iiii > 1;
select count() from test46006 where i+i < 1;
select i,count() from test46006 group by i order by i;
select count() from test46006 t1 left join test46006 t2 on t1.i=t2.i;

set enable_optimizer = 1;
-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from test46006;

-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from test46006 where toDate(p) > '2020-09-01';

-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from test46006 prewhere i = 1 where toDate(p) > '2020-09-01';

-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from test46006 prewhere i > 1;

-- optimized
select 'optimized:';

explain stats=0, verbose=0 select count() from test46006 where i < 1;

-- optimized
select 'optimized:';

explain stats=0, verbose=0 select * from (select count() as cnt from test46006 where i > 1) t where t.cnt < 10;

-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from (select i iiii from test46006) where iiii < 1 union all select count() from (select i iiii from test46006) where iiii < 1;

-- optimized
select 'optimized:';
explain stats=0, verbose=0 select count() from (select i iiii from test46006 prewhere iiii < 10 where i < 10) where iiii>1;

-- non-optimized
select 'non-optimized:';
explain stats=0, verbose=0 select count() from (select i iiii from test46006 prewhere iiii + i < 10 where j + iiii < 10) where iiii > 1;

-- non-optimized
select 'non-optimized:';
explain stats=0, verbose=0 select count() from test46006 where i+i < 1;

-- non-optimized
select 'non-optimized:';
explain stats=0, verbose=0 select i,count() from test46006 group by i;

-- non-optimized
select 'non-optimized:';
explain stats=0, verbose=0 select count() from test46006 t1 left join test46006 t2 on t1.i=t2.i;

explain stats=0, verbose=0
SELECT count(*)
FROM
(
    SELECT
        i,
        rowNumberInAllBlocks() AS rn
    FROM test46006
    WHERE cityHash64(intDiv(rn, 8192)) < 0.02
);

explain stats=0, verbose=0 
SELECT count(*)
FROM
(
    SELECT
        host() as x
    FROM test46006
    WHERE x = 'xx'
);

explain stats=0, verbose=0 
SELECT count(*)
FROM test46006
WHERE j <= 1
UNION ALL
SELECT count(*)
FROM test46006;

SELECT count(*)
FROM test46006
WHERE j <= 1
UNION ALL
SELECT count(*)
FROM test46006;

drop table if exists test46006_1;
create table test46006_1(i int) ENGINE = CnchMergeTree() partition by i order by i;
insert into test46006_1 select count() from test46006;
insert into test46006_1 select count() from test46006 where i<10;
drop table if exists test46006_1;

drop table if exists test46006;
