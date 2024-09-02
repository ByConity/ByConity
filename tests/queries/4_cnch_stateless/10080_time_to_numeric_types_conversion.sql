select toUInt32(toTimeType('05:00:00', 5));
select toUInt32(toTimeType('05:00:00.555', 5));
select toUInt32(toTimeType('05:00:00.555'));

select toUInt64(toTimeType('05:00:00', 5));
select toUInt64(toTimeType('05:00:00.555', 5));

select toInt32(toTimeType('05:00:00', 5));
select toInt32(toTimeType('05:00:00.555', 5));
select toInt32(toTimeType('05:00:00.555'));

select toInt64(toTimeType('05:00:00', 5));
select toInt64(toTimeType('05:00:00.555', 5));
select toInt64(toTimeType('05:00:00.555'));

select toFloat64(toTimeType('05:00:00.555', 5));
select toFloat32(toTimeType('05:00:00.555', 5));
select toFloat64(toTimeType('05:00:00.555'));
select toFloat32(toTimeType('05:00:00.555'));

select toUInt32('05:00:00'::Time);
select toUInt32('05:00:00.555'::Time);

select toUInt64('05:00:00'::Time);
select toUInt64('05:00:00.555'::Time);

select toInt32('05:00:00'::Time);
select toInt32('05:00:00.555'::Time);

select toInt64('05:00:00'::Time);
select toInt64('05:00:00.555'::Time);

select toFloat64('05:00:00.55555'::Time);
select toFloat64('05:00:00.55555'::Time(5));
select toFloat32('05:00:00.555'::Time(5));
select toFloat32('05:00:00.555'::Time(3));
select toFloat32('05:00:00.55'::Time);

SELECT TIME(3);
SELECT TIME(20333);
SELECT TIME(359999);
SELECT TIME(360001);
SELECT TIME(3600010);
SELECT TIME(36000100);
SELECT TIME(360001000);
SELECT TIME(39990001000);
SELECT TIME(20231221010203);
SELECT TIME(20231221010203.222);
SELECT TIME(2023122101020341423443892324324); -- { serverError 407 }
