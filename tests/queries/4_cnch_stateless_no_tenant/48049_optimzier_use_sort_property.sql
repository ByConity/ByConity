CREATE TABLE test
(
    `a` String,
    `b` String,
    `c` String,
    `d` Nullable(String)
)
ENGINE = CnchMergeTree
ORDER BY (a, b, c)
SETTINGS storage_policy = 'cnch_default_hdfs', index_granularity = 8192;

set enable_optimizer = 1;
set enable_sorting_property = 1;

-- { echoOn }
explain select * from test order by a, b, c, d;

explain select * from test order by a desc, b desc, c desc, d desc;

explain select * from test order by a, b desc, c;

explain select * from test order by a desc, b, c desc;

explain select a, b, concat(c, d) as e from test order by a, b, e;

explain select a as e, b, concat(c, d) as f from test order by e, b, f;

explain select * from test where a = 'x' order by b, c, d;

explain select * from test where a = 'x' and d = 'z' order by d, b, c;
