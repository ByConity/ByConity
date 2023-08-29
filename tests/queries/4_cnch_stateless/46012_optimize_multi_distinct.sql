create database IF NOT EXISTS test;

use test;
DROP TABLE IF EXISTS test.multi_dist;

CREATE TABLE test.multi_dist (a UInt64, b UInt64, c UInt64) ENGINE = CnchMergeTree() PARTITION BY a ORDER BY a UNIQUE KEY a;
insert into test.multi_dist values(1,2,3)(1,3,4)(2,3,4)(2,4,5)(3,4,5);

set enable_mark_distinct_optimzation=1;
select a,sum(distinct b),sum(distinct c),count() from test.multi_dist group by a order by a;
SELECT count(distinct(a)), sum(b), c FROM test.multi_dist GROUP BY c order by count(distinct(a));

set enable_optimizer=1;

explain select a,sum(distinct b),sum(distinct c),count() from test.multi_dist group by a;
explain SELECT count(distinct(a)), sum(b), c FROM test.multi_dist GROUP BY c;


DROP TABLE IF EXISTS test.multi_dist;
