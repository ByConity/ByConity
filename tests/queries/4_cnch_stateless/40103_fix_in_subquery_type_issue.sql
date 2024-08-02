drop table if exists t40103;

create table t40103 (k Int32, i1 Int32, i2 Nullable(Int32), s1 String, s2 Nullable(String)) engine = CnchMergeTree() order by tuple();

insert into t40103 values (1, 1, 1, '1', '1') , (2, 2, 2, 'a', 'a') , (3, 3, NULL, '3', NULL) ;

-- { echoOn }
/* test incompatible types */
select k, i1 in (select s1 from t40103) from t40103 order by k;

-- bsp mode generate different output
-- select k, s1 in (select i1 from t40103) from t40103 order by k;

/* test null */
select k, i1 in (select i2 from t40103) from t40103 order by k;

-- slightly different
-- select k, i2 in (select i1 from t40103) from t40103 order by k;

-- select k, i2 in (select i2 from t40103) from t40103 order by k;

-- select k, i2 in (select i2 from t40103) from t40103 order by k settings transform_null_in=1;

/* test tuple */
select k, (i1, i1) in (select s1, s1 from t40103) from t40103 order by k;

select k, (s1, s1) in (select i1, i1 from t40103) from t40103 order by k;

-- slightly different
-- select k, (i1, i2) in (select s1, s2 from t40103) from t40103 order by k;

select k, (i1, i1) in (select i2, i2 from t40103) from t40103 order by k;

select k, (i2, i2) in (select i1, i1 from t40103) from t40103 order by k;

-- slightly different
-- select k, (i2, i2) in (select i2, i2 from t40103) from t40103 order by k;

-- select k, (i2, i2) in (select i2, i2 from t40103) from t40103 order by k settings transform_null_in=1;

-- { echoOff }
drop table if exists t40103;
