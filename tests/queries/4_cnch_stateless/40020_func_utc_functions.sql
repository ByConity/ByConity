select number, (utc_timestamp() == now('UTC')) from (select * from numbers(10));
select number, (utc_date() == toDate(now('UTC'))) from (select * from numbers(10));
select number, (utc_time() == toTimeType(toString(now('UTC')))) from (select * from numbers(10));
