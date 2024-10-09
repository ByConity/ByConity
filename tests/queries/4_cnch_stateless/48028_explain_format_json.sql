DROP TABLE IF EXISTS t48028;
DROP TABLE IF EXISTS t480282;
set enable_optimizer=1;
CREATE TABLE t48028 (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;

CREATE TABLE t480282 (a UInt32, b UInt32) ENGINE = CnchMergeTree() partition by a order by a;

insert into t48028 values(1,2)(2,3)(3,4);
insert into t480282 values(1,2)(2,3)(3,4);

-- FORMAT Null since PlanNodeId is not stable
explain json=1 select t1.a, t2.b from t48028 t1 join t480282 t2 on t1.a=t2.a FORMAT Null;

set cte_mode='SHARED';
explain json=1 with t1 as (select * from t48028) select t1.a, t2.b from t1 t1 join t1 t2 on t1.a=t2.a FORMAT Null;

explain distributed json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a FORMAT Null;

explain analyze json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain analyze distributed json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain pb_json=1 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a FORMAT Null;
explain pb_json=1, add_whitespace=0 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a FORMAT Null;

explain analyze pipeline aggregate_profiles=1, json=1 with t1 as (select * from t48028) select t1.a, t2.b from t1 t1 join t1 t2 on t1.a=t2.a format Null;

explain analyze pipeline aggregate_profiles=1, json=0 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain analyze pipeline aggregate_profiles=0, json=1 with t1 as (select * from t48028) select t1.a, t2.b from t1 t1 join t1 t2 on t1.a=t2.a format Null;

explain analyze pipeline aggregate_profiles=0, json=0 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

explain analyze verbose = 0, profile = 0, stats = 0 insert into t48028 (*) select * from t480282;

select count() from t48028;

explain analyze distributed segment_id = 1, profile = 0, verbose = 0, stats = 0 select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

EXPLAIN ANALYZE distributed segment_profile = 1, profile = 0, verbose = 0, stats = 0, indexes = 1  select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

EXPLAIN ANALYZE distributed selected_parts=1, profile = 0, verbose = 0, stats = 0, indexes = 1  select t1.a, t2.b, t2.a+1 from t48028 t1 join t480282 t2 on t1.a=t2.a format Null;

select count() from t48028 settings log_explain_analyze_type = 'QUERY_PIPELINE' format Null;
select count() from t48028 settings log_explain_analyze_type = 'AGGREGATED_QUERY_PIPELINE' format Null;

DROP TABLE IF EXISTS t48028;
DROP TABLE IF EXISTS t480282;
