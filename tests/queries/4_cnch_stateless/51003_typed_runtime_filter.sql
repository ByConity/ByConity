DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (
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
    `decimal32` Decimal32(5),
    `decimal64` Decimal64(10),
    `decimal128` Decimal128(20),
    `bitmap` BitMap64,
    `array` Array(Int8),
    `tuple` Tuple(Int8, String),
    `map` Map(Int8, String),

    `ni8` Nullable(Int8),
    `ni16` Nullable(Int16),
    `ni32` Nullable(Int32),
    `ni64` Nullable(Int64),
    `ni128` Nullable(Int128),
    `ni256` Nullable(Int256),
    `nu8` Nullable(UInt8),
    `nu16` Nullable(UInt16),
    `nu32` Nullable(UInt32),
    `nu64` Nullable(UInt64),
    `nu128` Nullable(UInt128),
    `nu256` Nullable(UInt256),
    `nuuid` Nullable(UUID),
    `nf32` Nullable(Float32),
    `nf64` Nullable(Float64),
    `ndate` Nullable(Date),
    `ndate32` Nullable(Date32),
    `ndatetime` Nullable(DateTime),
    `ndatetime64` Nullable(DateTime64(3)),
    `nstr` Nullable(String),
    `nfxstr` Nullable(FixedString(20)),
    `nstrlc` LowCardinality(Nullable(String)),
    `nfxstrlc` LowCardinality(Nullable(FixedString(20))),
    `ndecimal32` Nullable(Decimal32(5)),
    `ndecimal64` Nullable(Decimal64(10)),
    `ndecimal128` Nullable(Decimal128(20)),
    `nbitmap` BitMap64,
    `narray` Nullable(Array(Int8)),
    `ntuple` Tuple(Int8, String),
    `nmap` Map(Int8, String)

) ENGINE = CnchMergeTree()
ORDER BY id;

CREATE TABLE t2 AS t1;

insert into t1 (
    id, i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,

    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap
) select
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    generateUUIDv4(),
    number,
    number,
    number,
    number,
    number,
    number,
    toString(number),
    toString(number),
    toString(number),
    toString(number),
    number,
    number,
    number,
    arrayToBitmap([number]),
    [number],
    tuple(number, toString(number)),
    map(number, toString(number)),

    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    generateUUIDv4(),
    number,
    number,
    number,
    number,
    number,
    number,
    toString(number),
    toString(number),
    toString(number),
    toString(number),
    number,
    number,
    number,
    arrayToBitmap([number]),
    [number],
    tuple(number, toString(number)),
    map(number, toString(number))
from
    system.numbers
limit
    10000;

insert into t2 select * from t1 where id >= 0 and id < 10;

set enable_optimizer=1, enum_repartition=0, enable_optimizer_fallback=0; -- enforce broadcast

create stats t1 Format Null;
create stats t2 Format Null;

-- test local
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = 0;

-- test local + bf
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = 0
settings runtime_filter_in_build_threshold=0;

-- test distributed
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = 0
settings enable_local_runtime_filter=0;

-- test distributed+bf
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = 0
settings enable_local_runtime_filter=0, runtime_filter_in_build_threshold=0;

-- test bypass bail out
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = 0
settings enable_local_runtime_filter=0, runtime_filter_in_build_threshold=0, runtime_filter_bloom_build_threshold=0;

-- test bypass no data
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = -1;

-- test bypass no data + bf
select count() from t1 join t2 using (i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, u128, u256, uuid, f32, f64, date,
    date32, datetime, datetime64, str, fxstr, strlc, fxstrlc,
    decimal32, decimal64, decimal128, bitmap, array, tuple, map,
    ni8, ni16, ni32, ni64, ni128, ni256, nu8, nu16, nu32, nu64, nu128, nu256, nuuid, nf32, nf64, ndate,
    ndate32, ndatetime, ndatetime64, nstr, nfxstr, nstrlc, nfxstrlc,
    ndecimal32, ndecimal64, ndecimal128, nbitmap, narray, ntuple, nmap) where t2.id = -1
settings enable_local_runtime_filter=0;
