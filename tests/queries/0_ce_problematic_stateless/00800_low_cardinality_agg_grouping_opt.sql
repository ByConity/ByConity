drop table if exists test.lc_group_opt_test;
-- 
CREATE TABLE test.lc_group_opt_test
(
    `c1` LowCardinality(Nullable(String)),
    `c2` LowCardinality(Nullable(String)),
    `cc1` GlobalLowCardinality(Nullable(String)),
    `cc2` GlobalLowCardinality(Nullable(String)),
    c3 Nullable(String),
    c4 Nullable(String)
)
ENGINE = MergeTree
ORDER BY c1;

INSERT INTO test.lc_group_opt_test SELECT   toString(number % 8) AS c1,     toString(number % 6) AS c2,     toString(number % 8) AS cc1,     toString(number % 6) AS cc2,     toString(number % 10000) AS c3,     toString(number % 10000) AS c4 FROM system.numbers LIMIT 100000;
select c1,c2,count() from test.lc_group_opt_test group by c1,c2 order by c1,c2 settings enable_batch_adaptive_aggregation=1;
select c1,c2,count() from test.lc_group_opt_test group by c1,c2 order by c1,c2 settings enable_batch_adaptive_aggregation=0;
select c1,c2,cc1,count() from test.lc_group_opt_test group by c1,c2,cc1 order by c1,c2,cc1 settings enable_batch_adaptive_aggregation=1;
select c1,c2,cc1,count() from test.lc_group_opt_test group by c1,c2,cc1 order by c1,c2,cc1 settings enable_batch_adaptive_aggregation=0;
select c3,c4,count() from test.lc_group_opt_test group by c3,c4 order by c3,c4 limit 10 settings enable_batch_adaptive_aggregation=1;

drop table if exists test.lc_group_opt_test;
