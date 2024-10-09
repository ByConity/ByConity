SET enable_optimizer=1;
SET enable_materialized_view_rewrite=1;

DROP TABLE IF EXISTS t40056_mv1;
DROP TABLE IF EXISTS t40056_mv2;
DROP TABLE IF EXISTS t40056_base;
DROP TABLE IF EXISTS t40056_target1;
DROP TABLE IF EXISTS t40056_target2;

CREATE TABLE t40056_base(a Int32, b Int32, c Int32) ENGINE = CnchMergeTree() order by a;
CREATE TABLE t40056_target1(a Int32, b Int32, sum_c AggregateFunction(sum, Int32)) ENGINE = CnchAggregatingMergeTree() order by a;
CREATE TABLE t40056_target2(a Int32, b Int32, sum_c AggregateFunction(sum, Int32)) ENGINE = CnchAggregatingMergeTree() order by b;
CREATE MATERIALIZED VIEW t40056_mv1 TO t40056_target1(a Int32, b Int32, sum_c AggregateFunction(sum, Int32)) AS SELECT a, b, sumState(c) as sum_c FROM t40056_base GROUP BY a, b;
CREATE MATERIALIZED VIEW t40056_mv2 TO t40056_target2(a Int32, b Int32, sum_c AggregateFunction(sum, Int32)) AS SELECT a, b, sumState(c) as sum_c FROM t40056_base GROUP BY a, b;

EXPLAIN stats=0 SELECT a, b, sum(c) FROM t40056_base WHERE a = 1 GROUP BY a, b;
EXPLAIN stats=0 SELECT a, b, sum(c) FROM t40056_base WHERE b = 1 GROUP BY a, b;

DROP TABLE IF EXISTS t40056_mv1;
DROP TABLE IF EXISTS t40056_mv2;
DROP TABLE IF EXISTS t40056_base;
DROP TABLE IF EXISTS t40056_target1;
DROP TABLE IF EXISTS t40056_target2;