select toTimeType(Time'10:00:00.123');
select toTimeType('10:00:00.123');
select toTimeType(Date'2022-01-01');
select toTimeType('2022-01-01');
select toTimeType('2022-01-01 20:00:00');
select toTimeType(DateTime'2022-01-01 20:00:00');
select toTimeType('2022-01-01 20:00:00'::DateTime(3));
select toTimeType('2022-01-01 20:00:00.1234567'::DateTime64(7), 7);
select toTimeType('2022-01-01 20:00:00.1234567'::DateTime64(7), 2);
select toTimeType('2022-01-01 20:00:00.1234567'::DateTime64(2), 4);

select toDate('20:00:00.123456'::Time()) == today();
select toDate(toDateTime('20:00:00.123456'::Time())) == today();
select Time(toDateTime('20:00:00.123456'::Time())) == '20:00:00.000';
select Time(toDateTime64('20:00:00.123456'::Time(6))) == '20:00:00.123';
