select cast(map('foo', 1), 'Map(String, Int64)');


select cast(map('foo', 1), 'Map(String, String)');
select cast(map('foo', 1), 'Map(String, Float32)');
select cast(map('100', '200'), 'Map(Int64, Int64)');
SELECT cast(map(toInt64OrZero(NULL), toFloat64OrZero(1)), 'Map(Int64, Float64)');

create database if not exists test;
drop table if exists t11012;
create table t11012(i Int32, m Map(String, Int32)) engine = CnchMergeTree() order by tuple();
insert into t11012 values (1, {'foo': 1});

select cast(m, 'Map(String, Int64)') from t11012;
select cast(m, 'Map(String, String)') from t11012;
select cast(m, 'Map(String, Float32)') from t11012;

select map('foo', cast(value, 'Int64')) from (select 0 as value from system.one) SETTINGS enable_optimizer=1;
