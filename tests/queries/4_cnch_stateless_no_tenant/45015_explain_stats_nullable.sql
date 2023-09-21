set enable_optimizer=1;
set create_stats_time_output=0;
use test;

drop table if exists test_explain_stats_n_local;
drop table if exists test_explain_stats_n;
create table test_explain_stats_n (
    id UInt64,
    i8 Nullable(Int8),
    i16 Nullable(Int16),
    i32 Nullable(Int32),
    i64 Nullable(Int64),
    u8 Nullable(UInt8),
    u16 Nullable(UInt16),
    u32 Nullable(UInt32),
    u64 Nullable(UInt64),
    f32 Nullable(Float32),
    f64 Nullable(Float64),
    d Nullable(Date),
    dt Nullable(DateTime),
    dt64 Nullable(DateTime64(3)),
    s Nullable(String),
    fxstr Nullable(FixedString(20)),
    decimal32 Nullable(Decimal32(5)),
    decimal64 Nullable(Decimal64(10)),
    decimal128 Nullable(Decimal128(20))
) ENGINE=CnchMergeTree() order by id;

insert into test_explain_stats_n values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_n values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_n values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats_n values (2, 2, 2, 2, 2, 2, 2, 2, 2, 0.2, 0.02, '2020-02-02', '2020-02-02 00:00:00', '2020-02-02 00:00:02.01','2', '2', 0.2, 0.02, 0.002);
insert into test_explain_stats_n(id) values (3);

set statistics_enable_sample=0;
create stats test_explain_stats_n;
explain (select id from test_explain_stats_n where i8 = 1) 
union all (select id from test_explain_stats_n where i16 = 1) 
union all (select id from test_explain_stats_n where i32 = 1) 
union all (select id from test_explain_stats_n where i64 = 1)
union all (select id from test_explain_stats_n where u8 = 1) 
union all (select id from test_explain_stats_n where u16 = 1)
union all (select id from test_explain_stats_n where u32 = 1)
union all (select id from test_explain_stats_n where u64 = 1)
union all (select id from test_explain_stats_n where f32 < 0.10001)
union all (select id from test_explain_stats_n where f64 < 0.01001)
union all (select id from test_explain_stats_n where d = '2020-01-01')
union all (select id from test_explain_stats_n where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_n where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_n where s = '1')
union all (select id from test_explain_stats_n where fxstr = '1')
union all (select id from test_explain_stats_n where decimal32 < 0.10001)
union all (select id from test_explain_stats_n where decimal64 < 0.010001)
union all (select id from test_explain_stats_n where decimal128 < 0.0010001);
drop stats test_explain_stats_n;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='NEVER';
create stats test_explain_stats_n;
explain (select id from test_explain_stats_n where i8 = 1) 
union all (select id from test_explain_stats_n where i16 = 1) 
union all (select id from test_explain_stats_n where i32 = 1) 
union all (select id from test_explain_stats_n where i64 = 1)
union all (select id from test_explain_stats_n where u8 = 1) 
union all (select id from test_explain_stats_n where u16 = 1)
union all (select id from test_explain_stats_n where u32 = 1)
union all (select id from test_explain_stats_n where u64 = 1)
union all (select id from test_explain_stats_n where f32 < 0.10001)
union all (select id from test_explain_stats_n where f64 < 0.01001)
union all (select id from test_explain_stats_n where d = '2020-01-01')
union all (select id from test_explain_stats_n where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_n where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_n where s = '1')
union all (select id from test_explain_stats_n where fxstr = '1')
union all (select id from test_explain_stats_n where decimal32 < 0.10001)
union all (select id from test_explain_stats_n where decimal64 < 0.010001)
union all (select id from test_explain_stats_n where decimal128 < 0.0010001);
drop stats test_explain_stats_n;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='ALWAYS';
create stats test_explain_stats_n;
explain (select id from test_explain_stats_n where i8 = 1) 
union all (select id from test_explain_stats_n where i16 = 1) 
union all (select id from test_explain_stats_n where i32 = 1) 
union all (select id from test_explain_stats_n where i64 = 1)
union all (select id from test_explain_stats_n where u8 = 1) 
union all (select id from test_explain_stats_n where u16 = 1)
union all (select id from test_explain_stats_n where u32 = 1)
union all (select id from test_explain_stats_n where u64 = 1)
union all (select id from test_explain_stats_n where f32 < 0.10001)
union all (select id from test_explain_stats_n where f64 < 0.01001)
union all (select id from test_explain_stats_n where d = '2020-01-01')
union all (select id from test_explain_stats_n where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats_n where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats_n where s = '1')
union all (select id from test_explain_stats_n where fxstr = '1')
union all (select id from test_explain_stats_n where decimal32 < 0.10001)
union all (select id from test_explain_stats_n where decimal64 < 0.010001)
union all (select id from test_explain_stats_n where decimal128 < 0.0010001);
drop stats test_explain_stats_n;

drop table if exists test_explain_stats_n;