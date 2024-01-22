select cast(map('foo', 1), 'Map(String, Int64)');

select cast(map('foo', 1), 'Map(String, String)');
select cast(map('foo', 1), 'Map(String, Float32)');
select cast(map('100', '200'), 'Map(Int64, Int64)');
SELECT cast(map(toInt64OrZero(NULL), toFloat64OrZero(1)), 'Map(Int64, Float64)');

drop table if exists 11012_map_conversion;
create table 11012_map_conversion(i Int32, m Map(String, Int32)) engine = CnchMergeTree() order by tuple();
insert into 11012_map_conversion values (1, {'foo': 1});

select cast(m, 'Map(String, Int64)') from 11012_map_conversion;
select cast(m, 'Map(String, String)') from 11012_map_conversion;
select cast(m, 'Map(String, Float32)') from 11012_map_conversion;

select map('foo', cast(value, 'Int64')) from (select 0 as value from system.one);