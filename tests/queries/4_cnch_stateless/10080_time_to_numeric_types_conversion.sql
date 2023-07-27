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
select toFloat32('05:00:00.55'::Time);