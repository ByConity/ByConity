select extract(year from '12:00:00.4566'::Time(4)); -- { serverError 43 }
select extract(month from '12:00:00.4566'::Time(4)); -- { serverError 43 }
select extract(day from '12:00:00.4566'::Time(4)); -- { serverError 43 }
select extract(hour from '12:00:50.4566'::Time(4));
select extract(minute from '12:20:00.4566'::Time(4));
select extract(second from '12:20:30.4566'::Time(4));