-- #
-- # Nested Loops semi-join subquery evaluation tests
-- #

-- # This portion of the file vas developed when subquery materialization
-- # was rule-based; to preserve the intended test scenarios, we switch
-- # off cost-based choice for them.
-- set @old_opt_switch=@@optimizer_switch;
-- set optimizer_switch='subquery_materialization_cost_based=off';

--disable_warnings
drop table if exists t0, t1, t2, t10, t11, t12;
--enable_warnings

-- #
-- # IN subquery optimization test
-- #
create table t1 (a int not null, b int, primary key (a));
create table t2 (a int not null, primary key (a));
create table t3 (a int not null, b int, primary key (a));
insert into t1 values (1,10), (2,20), (3,30),  (4,40);
insert into t2 values (2), (3), (4), (5);
insert into t3 values (10,3), (20,4), (30,5);
select * from t2 where t2.a in (select a from t1);
explain select * from t2 where t2.a in (select a from t1);
select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
explain select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
select * from t2 where t2.a in (select t1.a from t1,t3 where t1.b=t3.a);
explain select * from t2 where t2.a in (select t1.a from t1,t3 where t1.b=t3.a);
drop table t1, t2, t3;
create table t1 (a int, b int, index a (a,b));
create table t2 (a int, index a (a));
create table t3 (a int, b int, index a (a));
insert into t1 values (1,10), (2,20), (3,30), (4,40);
-- # making table large enough
create table t0(a int);
insert into t0 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
insert into t1
select rand()*100000+200,rand()*100000 from t0 A, t0 B, t0 C, t0 D;

insert into t2 values (2), (3), (4), (5);
insert into t3 values (10,3), (20,4), (30,5);
select * from t2 where t2.a in (select a from t1);
explain select * from t2 where t2.a in (select a from t1);
select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
explain select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
select * from t2 where t2.a in (select t1.a from t1,t3 where t1.b=t3.a);
explain select * from t2 where t2.a in (select t1.a from t1,t3 where t1.b=t3.a);
insert into t1 values (3,31);
select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
select * from t2 where t2.a in (select a from t1 where t1.b <> 30 and t1.b <> 31);
explain select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
drop table t0, t1, t2, t3;


-- #
-- # 1. Subqueries that are converted into semi-joins
-- #
create table t0 (a int);
insert into t0 values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

create table t1(a int, b int);
insert into t1 values (0,0),(1,1),(2,2);
create table t2 as select * from t1;

create table t11(a int, b int);

create table t10 (pk int, a int, primary key(pk));
insert into t10 select a,a from t0;
create table t12 like t10;
insert into t12 select * from t10;


--echo Flattened because of dependency, t10=func(t1)
explain select * from t1 where a in (select pk from t10);
select * from t1 where a in (select pk from t10);

--echo A confluent case of dependency
explain select * from t1 where a in (select a from t10 where pk=12);
select * from t1 where a in (select a from t10 where pk=12);

explain select * from t1 where a in (select a from t10 where pk=9);
select * from t1 where a in (select a from t10 where pk=9);

--echo An empty table inside
explain select * from t1 where a in (select a from t11);
select * from t1 where a in (select a from t11);

explain select * from t1 where a in (select pk from t10) and b in (select pk from t10);
select * from t1 where a in (select pk from t10) and b in (select pk from t10);

--echo flattening a nested subquery
explain select * from t1 where a in (select pk from t10 where t10.a in (select pk from t12));
select * from t1 where a in (select pk from t10 where t10.a in (select pk from t12));

--echo flattening subquery w/ several tables
explain select * from t1 where a in (select t10.pk from t10, t12 where t12.pk=t10.a);

--echo subqueries within outer joins go into ON expr.
-- # TODO: psergey: check if case conversions like those are ok (it broke on  windows)
--replace_result a A b B
explain
select * from t1 left join (t2 a, t2 b) on ( a.a= t1.a and b.a in (select pk from t10));

-- # TODO: psergey: check if case conversions like those are ok (it broke on  windows)
--echo t2 should be wrapped into OJ-nest, so we have "t1 LJ (t2 J t10)"
--replace_result a A b B
explain
select * from t1 left join t2 on (t2.a= t1.a and t2.a in (select pk from t10));

--echo we shouldn't flatten if we're going to get a join of > MAX_TABLES.
explain select * from 
  t1 s00, t1 s01,  t1 s02, t1 s03, t1 s04,t1 s05,t1 s06,t1 s07,t1 s08,t1 s09,
  t1 s10, t1 s11,  t1 s12, t1 s13, t1 s14,t1 s15,t1 s16,t1 s17,t1 s18,t1 s19,
  t1 s20, t1 s21,  t1 s22, t1 s23, t1 s24,t1 s25,t1 s26,t1 s27,t1 s28,t1 s29,
  t1 s30, t1 s31,  t1 s32, t1 s33, t1 s34,t1 s35,t1 s36,t1 s37,t1 s38,t1 s39,
  t1 s40, t1 s41,  t1 s42, t1 s43, t1 s44,t1 s45,t1 s46,t1 s47,t1 s48,t1 s49
where
  s00.a in (
  select m00.a from
    t1 m00, t1 m01,  t1 m02, t1 m03, t1 m04,t1 m05,t1 m06,t1 m07,t1 m08,t1 m09,
    t1 m10, t1 m11,  t1 m12, t1 m13, t1 m14,t1 m15,t1 m16,t1 m17,t1 m18,t1 m19
  );

select * from
  t1 left join t2 on (t2.a= t1.a and t2.a in (select pk from t10)) 
where t1.a < 5;
