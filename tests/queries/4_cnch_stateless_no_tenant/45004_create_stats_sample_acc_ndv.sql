set create_stats_time_output=0;
set statistics_enable_sample=1;
set statistics_accurate_sample_ndv='ALWAYS';
create table tb (
                          `id` UInt64,
                          `i8` Int8,
                          `i16` Int16,
                          `i32` Int32,
                          `i64` Int64,
                          `i128` Int128,
                          `i256` Int256,
                          `u8` UInt8,
                          `u16` UInt16,
                          `u32` UInt32,
                          `u64` UInt64,
                          `u128` UInt128,
                          `u256` UInt256,
                          `uuid` UUID,
                          `f32` Float32,
                          `f64` Float64,
                          `date` Date,
                          `date32` Date32,
                          `datetime` DateTime,
                          `datetime64` DateTime64(3),
                          `str` String,
                          `fxstr` FixedString(20),
                          `strlc` LowCardinality(String),
                          `fxstrlc` LowCardinality(FixedString(20)),
                          `strglc` LowCardinality(String),
                          `fxstrglc` LowCardinality(FixedString(20)),
                          `decimal32` Decimal32(5),
                          `decimal64` Decimal64(10),
                          `decimal128` Decimal128(20),
                          `1.2.3` UInt8,
                          `hello[world]` UInt8
) Engine = CnchMergeTree() order by id;


create table tbnull (
                              `id` UInt64,
                              `i8null` Nullable(Int8),
                              `i16null` Nullable(Int16),
                              `i32null` Nullable(Int32),
                              `i64null` Nullable(Int64),
                              `i128null` Nullable(Int128),
                              `i256null` Nullable(Int256),
                              `u8null`  Nullable(UInt8),
                              `u16null` Nullable(UInt16),
                              `u32null` Nullable(UInt32),
                              `u64null` Nullable(UInt64),
                              `u128null` Nullable(UInt128),
                              `u256null` Nullable(UInt256),
                              `uuid` Nullable(UUID),
                              `f32null` Nullable(Float32),
                              `f64null` Nullable(Float64),
                              `datenull` Nullable(Date),
                              `date32null` Nullable(Date32),
                              `datetimenulll` Nullable(DateTime),
                              `datetime64nulll` Nullable(DateTime64(3)),
                              `strnull` Nullable(String),
                              `fxstrnull` Nullable(FixedString(20)),
                              `strlcnull` LowCardinality(Nullable(String)),
                              `fxstrlcnull` LowCardinality(Nullable(FixedString(20))),
                              `strglcnull` LowCardinality(Nullable(String)),
                              `fxstrglcnull` LowCardinality(Nullable(FixedString(20))),
                              `decimal32null` Nullable(Decimal32(5)),
                              `decimal64null` Nullable(Decimal64(10)),
                              `decimal128null` Nullable(Decimal128(20)),
                              `int_params` Map(String, Int64),
                              `str_params` Map(String, String)
) Engine = CnchMergeTree() order by id;


select '---------create empty stats';
create stats *;
create stats tbnull(`__int_params__'a'`, `__str_params__'a'`);
select '---------show empty stats';
show stats *;
show column_stats *;

insert into tb values (1, -1, -10, -100, -1000, -10000, -100000, 1, 10, 100, 1000, 10000, 100000, '12345678-1234-1234-1234-123456789abc', 0.1, 0.01, '2022-01-01', '2022-01-01', '2022-01-01 00:00:01', '2022-01-01 00:00:01.11', 'str1', 'str1', 'str1', 'str1', 'str1', 'str1', 0.1, 0.01, 0.001, 1, 0);
insert into tb values (2, -2, -20, -200, -2000, -20000, -200000, 2, 20, 200, 2000, 20000, 200000, '22345678-2234-2234-2234-223456789abc', 0.2, 0.02, '2022-02-02', '2022-02-02','2022-02-02 00:00:02', '2022-02-02 00:00:02.22', 'str2', 'str2', 'str2', 'str2', 'str2', 'str2', 0.2, 0.02, 0.002, 0, 1);

insert into tbnull values (1, -1, -10, -100, -1000, -10000, -100000, 1, 10, 100, 1000, 10000, 100000, '12345678-1234-1234-1234-123456789abc', 0.1, 0.01, '2022-01-01', '2022-01-01', '2022-01-01 00:00:01', '2022-01-01 00:00:01.11', 'str1', 'str1', 'str1', 'str1', 'str1', 'str1', 0.1, 0.01, 0.001, {'a': 100}, {'a': 'str1'});
insert into tbnull values (2, -2, -20, -200, -2000, -20000, -200000, 2, 20, 200, 2000, 20000, 200000, '22345678-2234-2234-2234-223456789abc', 0.2, 0.02, '2022-02-02', '2022-02-02','2022-02-02 00:00:02', '2022-02-02 00:00:02.22', 'str2', 'str2', 'str2', 'str2', 'str2', 'str2', 0.2, 0.02, 0.002, {'a': 200}, {'a': 'str2'});
insert into tbnull(id) values (3);

select '---------drop single stats';
drop stats tb;
select '---------show remaining stats';
show stats *;
select '---------create partial stats';
create stats if not exists *;
select '---------show partial stats';
show stats *;
show column_stats *;
select '---------create stats override';
create stats all;
select '---------show stats override';
show stats all;
show column_stats all;
select '---------drop stats all';
drop stats all;
select '---------show empty stats';
show stats all;
drop table tb;
drop table tbnull;
