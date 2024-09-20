set enable_optimizer = 1;

create database if not exists trim_cast_48057;
use trim_cast_48057;

drop table if exists tb;

create table tb (
    i64 Int64,
    i64null Nullable(Int64),
    i64lc LowCardinality(Int64),
    i64glc LowCardinality(Int64),
    i64lcnull LowCardinality(Nullable(Int64)),
    i64glcnull LowCardinality(Nullable(Int64)),
    u64 UInt64,
    u64null Nullable(UInt64),
    datetime DateTime,
    str String,
    fxstr FixedString(10)
) Engine = CnchMergeTree()
order by
    i64;

explain stats = 0
select
    toInt64(i64),
    toInt64(i64null),
    toInt64(i64lc),
    toInt64(i64glc),
    toInt64(i64lcnull),
    toInt64(i64glcnull),
    toInt64(u64),
    toInt64(u64null),
    toDateTime(datetime),
    toDateTime32(datetime),
    toString(str),
    toString(fxstr),
    toFixedString(fxstr, 10), -- TODO support trim FixedString if size is equal
    toFixedString(fxstr, 12)
from
    tb;

explain stats = 0
select
    toInt64OrZero(i64),
    toInt64OrZero(i64null),
    toInt64OrZero(i64lc),
    toInt64OrZero(i64glc),
    toInt64OrZero(i64lcnull),
    toInt64OrZero(i64glcnull),
    toInt64OrZero(u64),
    toInt64OrZero(u64null),
    toDateTimeOrZero(datetime)
from
    tb;

explain stats = 0
select
    toInt64OrNull(i64),
    toInt64OrNull(i64null),
    toInt64OrNull(i64lc),
    toInt64OrNull(i64glc),
    toInt64OrNull(i64lcnull),
    toInt64OrNull(i64glcnull),
    toInt64OrNull(u64),
    toInt64OrNull(u64null),
    toDateTimeOrNull(datetime)
from
    tb;

explain stats=0
select
    cast(i64, 'Int64'),
    cast(i64null, 'Int64'),
    cast(i64null, 'Int64'),
    cast(i64lc, 'Int64'),
    cast(i64glc, 'Int64'),
    cast(i64lcnull, 'Int64'),
    cast(i64glcnull, 'Int64'),
    cast(u64, 'Int64'),
    cast(u64null, 'Int64'),
    cast(str, 'Int64'),
    cast(fxstr, 'Int64'),
    cast(datetime, 'Int64'),
    cast(fxstr, 'FixedString(12)')
from
    tb;

explain stats=0
select
    cast(i64, 'Int64'),
    cast(i64null, 'Nullable(Int64)'),
    cast(i64lc, 'LowCardinality(Int64)'),
    cast(i64glc, 'LowCardinality(Int64)'),
    cast(i64lcnull, 'LowCardinality(Nullable(Int64))'),
    cast(i64glcnull, 'LowCardinality(Nullable(Int64))'),
    cast(u64, 'UInt64'),
    cast(u64null, 'Nullable(UInt64)'),
    cast(str, 'String'),
    cast(fxstr, 'FixedString(10)'),
    cast(datetime, 'DateTime'),
    cast(datetime, 'DateTime32')
from
    tb;

explain stats=0 select count() from tb as a inner join (select * from tb) as b on a.i64 = toInt64(b.i64);
explain stats=0 select count() from tb as a inner join (select * from tb) as b on a.i64 = cast(b.i64, 'Int64');

drop table if exists tb;