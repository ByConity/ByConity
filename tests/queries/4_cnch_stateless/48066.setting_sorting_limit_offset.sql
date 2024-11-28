use test;
DROP TABLE IF EXISTS test48066;
CREATE TABLE test48066 (i UInt64) Engine = CnchMergeTree() order by i;
set enable_optimizer=1;
INSERT INTO test48066 SELECT number FROM numbers(100);
INSERT INTO test48066 SELECT number FROM numbers(10,100);

-- Only set limit
SET limit = 5;
SELECT i FROM test48066 order by i; -- 5 rows
SELECT i FROM test48066  order by i OFFSET 20; -- 5 rows
SELECT i FROM (SELECT i FROM test48066 order by i LIMIT 10 OFFSET 50) TMP; -- 5 rows
SELECT i FROM test48066 order by i LIMIT 4 OFFSET 192; -- 4 rows

-- Only set offset
SET limit = 0;
SET offset = 195;
SELECT i FROM test48066 order by i; -- 5 rows
SELECT i FROM test48066 order by i OFFSET 20; -- no result
SELECT i FROM test48066 order by i LIMIT 100; -- no result
SET offset = 10;
SELECT i FROM test48066 order by i LIMIT 20 OFFSET 100; -- 10 rows
SELECT i FROM test48066 order by i LIMIT 11 OFFSET 100; -- 1 rows

-- offset and limit together
SET limit = 10;
SELECT i FROM test48066 order by i LIMIT 50 OFFSET 50; -- 10 rows
SELECT i FROM test48066 order by i LIMIT 50 OFFSET 190; -- 0 rows
SELECT i FROM test48066 order by i LIMIT 50 OFFSET 185; -- 5 rows
SELECT i FROM test48066 order by i LIMIT 18 OFFSET 5; -- 8 rows

DROP TABLE IF EXISTS test_final_sorting48066;
CREATE TABLE test_final_sorting48066 (a UInt64, b UInt64, C String) Engine = CnchMergeTree() order by a;

INSERT INTO test_final_sorting48066 SELECT number, number%5, toString(number%3) FROM numbers(20);
set enable_order_by_all = 1;

SET limit = 0;
set offset = 0;
select * from test_final_sorting48066 settings final_order_by_all_direction=1;
select * from test_final_sorting48066 settings final_order_by_all_direction=-1;

SET limit = 0;
SET offset = 10;
select * from test_final_sorting48066 settings final_order_by_all_direction=1;
select * from test_final_sorting48066 settings final_order_by_all_direction=-1;

SET limit = 10;
SET offset = 0;
select * from test_final_sorting48066 settings final_order_by_all_direction=1;
select * from test_final_sorting48066 settings final_order_by_all_direction=-1;

SET limit = 5;
SET offset = 5;
select * from test_final_sorting48066 settings final_order_by_all_direction=1;
select * from test_final_sorting48066 settings final_order_by_all_direction=-1;

select * from test_final_sorting48066 LIMIT 10 OFFSET 4 settings final_order_by_all_direction=1;
select * from test_final_sorting48066 LIMIT 10 OFFSET 4 settings final_order_by_all_direction=-1;

select * from test_final_sorting48066 order by a with fill settings final_order_by_all_direction=1;
select * from test_final_sorting48066 order by a with fill settings final_order_by_all_direction=-1;

set final_order_by_all_direction = -1;
select b from test_final_sorting48066 group by b union all select a from test_final_sorting48066 group by a;

set final_order_by_all_direction = 1;
select b from test_final_sorting48066 group by b union all select a from test_final_sorting48066 group by a;

set limit = 5;
set offset = 6;
select b from test_final_sorting48066 group by b union all select a from test_final_sorting48066 group by a;

set limit = 5;
set offset = 0;
set early_execute_in_subquery = 1;
select a from test_final_sorting48066 order by  a limit 1 union all select count() from test_final_sorting48066 where a in (select a from test_final_sorting48066 order by a limit 20);

DROP TABLE IF EXISTS test48066;
