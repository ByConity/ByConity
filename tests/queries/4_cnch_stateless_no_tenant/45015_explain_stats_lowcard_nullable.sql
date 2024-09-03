set enable_optimizer=1;
set enable_optimizer_fallback=0;

set create_stats_time_output=0;

drop table if exists test_explain_stats_nlc;
create table test_explain_stats_nlc (
    id UInt64,
    i8 LowCardinality(Nullable(Int8)),
    i16 LowCardinality(Nullable(Int16)),
    i32 LowCardinality(Nullable(Int32)),
    i64 LowCardinality(Nullable(Int64)),
    u8 LowCardinality(Nullable(UInt8)),
    u16 LowCardinality(Nullable(UInt16)),
    u32 LowCardinality(Nullable(UInt32)),
    u64 LowCardinality(Nullable(UInt64)),
    f32 LowCardinality(Nullable(Float32)),
    f64 LowCardinality(Nullable(Float64)),
    d LowCardinality(Nullable(Date)),
    d32 LowCardinality(Nullable(Date32)),
    dt LowCardinality(Nullable(DateTime)),
    dt64 Nullable(DateTime64(3)),  -- lowcard don't support decimal
    s LowCardinality(Nullable(String)),
    fxstr LowCardinality(Nullable(FixedString(20))),
    decimal32 Nullable(Decimal32(5)),
    decimal64 Nullable(Decimal64(10)),
    decimal128 Nullable(Decimal128(20))
) ENGINE=CnchMergeTree() order by id;

insert into test_explain_stats_nlc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_nlc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_nlc values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_nlc values (2, 2, 2, 2, 2, 2, 2, 2, 2, 0.2, 0.02, '2020-02-02', '2020-02-02', '2020-02-02 00:00:00', '2020-02-02 00:00:02.01','2', '2', 0.2, 0.02, 0.002);
insert into test_explain_stats_nlc(id) values (3);

set statistics_enable_sample=0;
create stats test_explain_stats_nlc;
explain (select id from test_explain_stats_nlc where i8 = 1) 
union all (select id from test_explain_stats_nlc where i16 = 1) 
union all (select id from test_explain_stats_nlc where i32 = 1) 
union all (select id from test_explain_stats_nlc where i64 = 1)
union all (select id from test_explain_stats_nlc where u8 = 1) 
union all (select id from test_explain_stats_nlc where u16 = 1)
union all (select id from test_explain_stats_nlc where u32 = 1)
union all (select id from test_explain_stats_nlc where u64 = 1)
union all (select id from test_explain_stats_nlc where f32 < 0.10001)
union all (select id from test_explain_stats_nlc where f64 < 0.01001)
union all (select id from test_explain_stats_nlc where d = '2020-01-01')
union all (select id from test_explain_stats_nlc where d32 = '2020-01-01')
union all (select id from test_explain_stats_nlc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_nlc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_nlc where s = '1')
union all (select id from test_explain_stats_nlc where fxstr = '1')
union all (select id from test_explain_stats_nlc where decimal32 < 0.10001)
union all (select id from test_explain_stats_nlc where decimal64 < 0.010001)
union all (select id from test_explain_stats_nlc where decimal128 < 0.0010001);
drop stats test_explain_stats_nlc;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='NEVER';
create stats test_explain_stats_nlc;
explain (select id from test_explain_stats_nlc where i8 = 1) 
union all (select id from test_explain_stats_nlc where i16 = 1) 
union all (select id from test_explain_stats_nlc where i32 = 1) 
union all (select id from test_explain_stats_nlc where i64 = 1)
union all (select id from test_explain_stats_nlc where u8 = 1) 
union all (select id from test_explain_stats_nlc where u16 = 1)
union all (select id from test_explain_stats_nlc where u32 = 1)
union all (select id from test_explain_stats_nlc where u64 = 1)
union all (select id from test_explain_stats_nlc where f32 < 0.10001)
union all (select id from test_explain_stats_nlc where f64 < 0.01001)
union all (select id from test_explain_stats_nlc where d = '2020-01-01')
union all (select id from test_explain_stats_nlc where d32 = '2020-01-01')
union all (select id from test_explain_stats_nlc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_nlc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_nlc where s = '1')
union all (select id from test_explain_stats_nlc where fxstr = '1')
union all (select id from test_explain_stats_nlc where decimal32 < 0.10001)
union all (select id from test_explain_stats_nlc where decimal64 < 0.010001)
union all (select id from test_explain_stats_nlc where decimal128 < 0.0010001);
drop stats test_explain_stats_nlc;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='ALWAYS';
create stats test_explain_stats_nlc;
explain (select id from test_explain_stats_nlc where i8 = 1) 
union all (select id from test_explain_stats_nlc where i16 = 1) 
union all (select id from test_explain_stats_nlc where i32 = 1) 
union all (select id from test_explain_stats_nlc where i64 = 1)
union all (select id from test_explain_stats_nlc where u8 = 1) 
union all (select id from test_explain_stats_nlc where u16 = 1)
union all (select id from test_explain_stats_nlc where u32 = 1)
union all (select id from test_explain_stats_nlc where u64 = 1)
union all (select id from test_explain_stats_nlc where f32 < 0.10001)
union all (select id from test_explain_stats_nlc where f64 < 0.01001)
union all (select id from test_explain_stats_nlc where d = '2020-01-01')
union all (select id from test_explain_stats_nlc where d32 = '2020-01-01')
union all (select id from test_explain_stats_nlc where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_nlc where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_nlc where s = '1')
union all (select id from test_explain_stats_nlc where fxstr = '1')
union all (select id from test_explain_stats_nlc where decimal32 < 0.10001)
union all (select id from test_explain_stats_nlc where decimal64 < 0.010001)
union all (select id from test_explain_stats_nlc where decimal128 < 0.0010001);
drop stats test_explain_stats_nlc;

drop table if exists test_explain_stats_nlc;