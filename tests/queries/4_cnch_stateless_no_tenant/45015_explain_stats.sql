set enable_optimizer=1;
set create_stats_time_output=0;
use test;

drop table if exists test_explain_stats;
create table test_explain_stats (
    id UInt64,
    i8 Int8,
    i16 Int16,
    i32 Int32,
    i64 Int64,
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64,
    f32 Float32,
    f64 Float64,
    d Date,
    dt DateTime,
    dt64 DateTime64(3),
    s String,
    fxstr FixedString(20),
    decimal32 Decimal32(5),
    decimal64 Decimal64(10),
    decimal128 Decimal128(20)
) ENGINE=CnchMergeTree() order by id;

insert into test_explain_stats values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats values (1, 1, 1, 1, 1, 1, 1, 1, 1, 0.1, 0.01, '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00.01','1', '1', 0.1, 0.01, 0.001);
insert into test_explain_stats values (2, 2, 2, 2, 2, 2, 2, 2, 2, 0.2, 0.02, '2020-02-02', '2020-02-02 00:00:00', '2020-02-02 00:00:02.01','2', '2', 0.2, 0.02, 0.002);

set statistics_enable_sample=0;
create stats test_explain_stats;
explain (select id from test_explain_stats where i8 = 1) 
union all (select id from test_explain_stats where i16 = 1) 
union all (select id from test_explain_stats where i32 = 1) 
union all (select id from test_explain_stats where i64 = 1)
union all (select id from test_explain_stats where u8 = 1) 
union all (select id from test_explain_stats where u16 = 1)
union all (select id from test_explain_stats where u32 = 1)
union all (select id from test_explain_stats where u64 = 1)
union all (select id from test_explain_stats where f32 < 0.10001)
union all (select id from test_explain_stats where f64 < 0.01001)
union all (select id from test_explain_stats where d = '2020-01-01')
union all (select id from test_explain_stats where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats where s = '1')
union all (select id from test_explain_stats where fxstr = '1')
union all (select id from test_explain_stats where decimal32 < 0.10001)
union all (select id from test_explain_stats where decimal64 < 0.010001)
union all (select id from test_explain_stats where decimal128 < 0.0010001);
drop stats test_explain_stats;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='NEVER';
create stats test_explain_stats;
explain (select id from test_explain_stats where i8 = 1) 
union all (select id from test_explain_stats where i16 = 1) 
union all (select id from test_explain_stats where i32 = 1) 
union all (select id from test_explain_stats where i64 = 1)
union all (select id from test_explain_stats where u8 = 1) 
union all (select id from test_explain_stats where u16 = 1)
union all (select id from test_explain_stats where u32 = 1)
union all (select id from test_explain_stats where u64 = 1)
union all (select id from test_explain_stats where f32 < 0.10001)
union all (select id from test_explain_stats where f64 < 0.01001)
union all (select id from test_explain_stats where d = '2020-01-01')
union all (select id from test_explain_stats where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats where s = '1')
union all (select id from test_explain_stats where fxstr = '1')
union all (select id from test_explain_stats where decimal32 < 0.10001)
union all (select id from test_explain_stats where decimal64 < 0.010001)
union all (select id from test_explain_stats where decimal128 < 0.0010001);
drop stats test_explain_stats;

set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='ALWAYS';
create stats test_explain_stats;
explain (select id from test_explain_stats where i8 = 1) 
union all (select id from test_explain_stats where i16 = 1) 
union all (select id from test_explain_stats where i32 = 1) 
union all (select id from test_explain_stats where i64 = 1)
union all (select id from test_explain_stats where u8 = 1) 
union all (select id from test_explain_stats where u16 = 1)
union all (select id from test_explain_stats where u32 = 1)
union all (select id from test_explain_stats where u64 = 1)
union all (select id from test_explain_stats where f32 < 0.10001)
union all (select id from test_explain_stats where f64 < 0.01001)
union all (select id from test_explain_stats where d = '2020-01-01')
union all (select id from test_explain_stats where dt = '2020-01-01 00:00:00')
union all (select id from test_explain_stats where dt64 = '2020-01-01 00:00:00.01')
union all (select id from test_explain_stats where s = '1')
union all (select id from test_explain_stats where fxstr = '1')
union all (select id from test_explain_stats where decimal32 < 0.10001)
union all (select id from test_explain_stats where decimal64 < 0.010001)
union all (select id from test_explain_stats where decimal128 < 0.0010001);
drop stats test_explain_stats;

drop table if exists test_explain_stats;