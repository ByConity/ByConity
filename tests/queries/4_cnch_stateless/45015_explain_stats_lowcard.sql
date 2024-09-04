set enable_optimizer=1;
set enable_optimizer_fallback=0;

set create_stats_time_output=0;

drop table if exists test_explain_stats_lc;
create table test_explain_stats_lc (
    id UInt64,
    i8 LowCardinality(Int8),
    i16 LowCardinality(Int16),
    i32 LowCardinality(Int32),
    i64 LowCardinality(Int64),
    u8 LowCardinality(UInt8),
    u16 LowCardinality(UInt16),
    u32 LowCardinality(UInt32),
    u64 LowCardinality(UInt64),
    f32 LowCardinality(Float32),
    f64 LowCardinality(Float64),
    d LowCardinality(Date),
    d32 LowCardinality(Date32),
    dt LowCardinality(DateTime),
    dt64 DateTime64(3),  -- lowcard don't support decimal
    s LowCardinality(String),
    fxstr LowCardinality(FixedString(20)),
    decimal32 Decimal32(5),
    decimal64 Decimal64(10),
    decimal128 Decimal128(20)
) ENGINE=CnchMergeTree() order by id;


insert into test_explain_stats_lc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_lc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_lc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_lc values (2, 2, 2, 2, 2, 2, 2, 2, 2, 0.2, 0.02, '2020-02-02', '2020-02-02', '2020-02-02 00:00:00', '2020-02-02 00:00:02.01','2', '2', 0.2, 0.02, 0.002);

set statistics_enable_sample=0;
create stats test_explain_stats_lc;
explain (select id from test_explain_stats_lc where i8 = 1) 
union all (select id from test_explain_stats_lc where i16 = 1) 
union all (select id from test_explain_stats_lc where i32 = 1) 
union all (select id from test_explain_stats_lc where i64 = 1)
union all (select id from test_explain_stats_lc where u8 = 1) 
union all (select id from test_explain_stats_lc where u16 = 1)
union all (select id from test_explain_stats_lc where u32 = 1)
union all (select id from test_explain_stats_lc where u64 = 1)
union all (select id from test_explain_stats_lc where f32 < 0.10001)
union all (select id from test_explain_stats_lc where f64 < 0.01001)
union all (select id from test_explain_stats_lc where d = '2020-01-01')
union all (select id from test_explain_stats_lc where d32 = '2020-01-01')
union all (select id from test_explain_stats_lc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_lc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_lc where s = '1')
union all (select id from test_explain_stats_lc where fxstr = '1')
union all (select id from test_explain_stats_lc where decimal32 < 0.10001)
union all (select id from test_explain_stats_lc where decimal64 < 0.010001)
union all (select id from test_explain_stats_lc where decimal128 < 0.0010001);
drop stats test_explain_stats_lc;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='NEVER';
create stats test_explain_stats_lc;
explain (select id from test_explain_stats_lc where i8 = 1) 
union all (select id from test_explain_stats_lc where i16 = 1) 
union all (select id from test_explain_stats_lc where i32 = 1) 
union all (select id from test_explain_stats_lc where i64 = 1)
union all (select id from test_explain_stats_lc where u8 = 1) 
union all (select id from test_explain_stats_lc where u16 = 1)
union all (select id from test_explain_stats_lc where u32 = 1)
union all (select id from test_explain_stats_lc where u64 = 1)
union all (select id from test_explain_stats_lc where f32 < 0.10001)
union all (select id from test_explain_stats_lc where f64 < 0.01001)
union all (select id from test_explain_stats_lc where d = '2020-01-01')
union all (select id from test_explain_stats_lc where d32 = '2020-01-01')
union all (select id from test_explain_stats_lc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_lc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_lc where s = '1')
union all (select id from test_explain_stats_lc where fxstr = '1')
union all (select id from test_explain_stats_lc where decimal32 < 0.10001)
union all (select id from test_explain_stats_lc where decimal64 < 0.010001)
union all (select id from test_explain_stats_lc where decimal128 < 0.0010001);
drop stats test_explain_stats_lc;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='ALWAYS';
create stats test_explain_stats_lc;
explain (select id from test_explain_stats_lc where i8 = 1) 
union all (select id from test_explain_stats_lc where i16 = 1) 
union all (select id from test_explain_stats_lc where i32 = 1) 
union all (select id from test_explain_stats_lc where i64 = 1)
union all (select id from test_explain_stats_lc where u8 = 1) 
union all (select id from test_explain_stats_lc where u16 = 1)
union all (select id from test_explain_stats_lc where u32 = 1)
union all (select id from test_explain_stats_lc where u64 = 1)
union all (select id from test_explain_stats_lc where f32 < 0.10001)
union all (select id from test_explain_stats_lc where f64 < 0.01001)
union all (select id from test_explain_stats_lc where d = '2020-01-01')
union all (select id from test_explain_stats_lc where d32 = '2020-01-01')
union all (select id from test_explain_stats_lc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_lc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_lc where s = '1')
union all (select id from test_explain_stats_lc where fxstr = '1')
union all (select id from test_explain_stats_lc where decimal32 < 0.10001)
union all (select id from test_explain_stats_lc where decimal64 < 0.010001)
union all (select id from test_explain_stats_lc where decimal128 < 0.0010001);
drop stats test_explain_stats_lc;

drop table if exists test_explain_stats_lc;