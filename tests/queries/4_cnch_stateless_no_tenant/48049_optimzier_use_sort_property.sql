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

explain select a as e, b, concat(c, d) as f from test order by e desc, b desc, f desc;

explain select * from test where a = 'x' order by b, c, d;

explain select * from test where a = 'x' order by b desc, c desc, d desc;

explain select * from test where a = 'x' and d = 'z' order by d, b, c;

explain select * from test where a = 'x' and d = 'z' order by d desc, b desc, c desc;

explain pipeline select * from test where a = 'x' order by b, c limit 10;

explain select a, b, c from (select a, b, c from test union all select a, b, c from test) order by a desc, b desc, c desc;

explain select a, b, c from (select a, b, c from test where a = '1' and b = '1' union all select a, b, c from test where a = '3' and b = '3') order by c desc limit 10;

-- { echoOff }
insert into test values ('1', '1', '1', '1');
insert into test values ('1', '1', '2', '2');

insert into test values ('3', '3', '3', '3');
insert into test values ('3', '3', '4', '4');

-- { echoOn }
select a as e, b, concat(c, d) as f from test order by e desc, b desc, f desc;

select a, b, c from (select a, b, c from test union all select a, b, c from test) order by a desc, b desc, c desc;

select a, b, c from (select a, b, c from test where a = '1' and b = '1' union all select a, b, c from test where a = '3' and b = '3') order by c desc limit 10;
