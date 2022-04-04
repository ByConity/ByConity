select array('12:00:00.4566'::Time, '12:00:00.4566'::Time);
select array('12:00:00.4566'::Time, '12:00:00.4566'::Time(4));
select toTypeName(array('12:00:00.4566'::Time, '12:00:00.4566'::Time(4)));

select tuple('12:00:00.4566'::Time, '12:00:00.4566'::Time, 'asv');
select tuple('12:00:00.4566'::Time, '12:00:00.4566'::Time(4), 12);
select toTypeName(tuple('12:00:00.4566'::Time, '12:00:00.4566'::Time(4), 107.95));
