drop table if exists test.lc_dict;
-- 

CREATE TABLE test.lc_dict
(
    `d2` UInt32,
    `d1` UInt32,
    `uintlc` LowCardinality(UInt32),
    `fltlc` LowCardinality(Nullable(Float32)),
    `strlc` LowCardinality(Nullable(String))
)
ENGINE = MergeTree
PARTITION BY d2
ORDER BY (d2, d1)
SETTINGS index_granularity = 8192, vertical_merge_algorithm_min_rows_to_activate = 100000, vertical_merge_algorithm_min_columns_to_activate = 2,min_bytes_for_wide_part=1048576;
-- low_cardinality_distinct_threshold = 100000
select 'test insert';
INSERT INTO test.lc_dict  SELECT intDiv(number,     5000000) AS d2,number AS d1,  (rand64() % 700000 +1)*10000 AS uint,    uint * pi() as flt, ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'][rand()%10+1] AS str FROM numbers(200000) order by rand();
INSERT INTO test.lc_dict  SELECT intDiv(number, 5000000) AS d2,number AS d1,  (rand64() % 700000 +1)*10000 AS uint,    uint * pi() as flt, ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'][rand()%10+1] AS str FROM numbers(200000) order by rand();
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict  group by t,_part order by _part; 

select 'test merge';
optimize table test.lc_dict final;
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict  group by t,_part order by _part;


drop table if exists test.lc_dict2;
CREATE TABLE test.lc_dict2
(
    `d2` UInt32,
    `d1` UInt32,
    `uintlc` LowCardinality(UInt64),
    `fltlc` Nullable(Float64),
    `strlc` Nullable(String)
)
ENGINE = MergeTree
PARTITION BY d2
ORDER BY (d2, d1)
SETTINGS index_granularity = 8192, vertical_merge_algorithm_min_rows_to_activate = 100000, vertical_merge_algorithm_min_columns_to_activate = 2,min_bytes_for_wide_part=1048576;

select 'test convert';
insert into test.lc_dict2 select * from test.lc_dict;
INSERT INTO test.lc_dict2  SELECT intDiv(number, 5000000) AS d2,number AS d1,  (rand64() % 700000 +1)*10000 AS uint,    uint * pi() as flt, ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'][rand()%10+1] AS str FROM numbers(200000) order by rand();
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict2  group by t,_part order by _part ;

select 'optimize converted';
optimize table test.lc_dict2 final;
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict2  group by t,_part order by _part;

select 'test convert 2';
insert into test.lc_dict select * from test.lc_dict2;
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict  group by t,_part order by _part;
optimize table test.lc_dict final;
select  lowCardinalityIsNoneEncoded(uintlc) as t,_part from test.lc_dict  group by t,_part order by _part;

select 'test query';
select * from test.lc_dict format Null;
select d2 from test.lc_dict where uintlc > 100 and d2 >= 0 order by uintlc limit 3;
select d2 from test.lc_dict where fltlc > 100 and d2 >= 0 order by strlc limit 3;
select d2 from test.lc_dict where strlc in ('one','two') and d2 >= 0 order by strlc,uintlc limit 3;
select 'test query mix part';
INSERT INTO test.lc_dict  SELECT intDiv(number, 5000000) AS d2,number AS d1,  (rand64() % 700000 +1)*10000 AS uint,    uint * pi() as flt, ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'][rand()%10+1] AS str FROM numbers(20000) order by rand();
select d2 from test.lc_dict where uintlc > 100 and d2 >= 0 order by uintlc limit 3;
select d2 from test.lc_dict where fltlc > 100 and d2 >= 0 order by strlc limit 3;
select d2 from test.lc_dict where strlc in ('one','two') and d2 >= 0 order by strlc,uintlc limit 3;

drop table if exists test.lc_dict;
drop table if exists test.lc_dict2;

select 'compact part test';
drop table if exists test.events_compact;
CREATE TABLE test.events_compact
(
    `app_id` UInt32,
    `string_params` Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree
PARTITION BY app_id
ORDER BY app_id
SETTINGS index_granularity = 8192;
insert into test.events_compact format JSONEachRow {"app_id":10000000,"string_params":{"__is_history":"true"}};
select * from test.events_compact;
drop table if exists test.events_compact;
