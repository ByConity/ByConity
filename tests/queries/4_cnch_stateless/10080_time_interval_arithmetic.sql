select '12:00:00.4566'::Time(2) + INTERVAL 24 HOUR;
select '12:00:00.4566'::Time(6) + INTERVAL 10 HOUR;

select '12:00:00.4566'::Time(6) + INTERVAL 10 MINUTE;
select '12:00:00.4566'::Time(6) + INTERVAL 100 MINUTE;

select '12:00:00.4566'::Time(6) + INTERVAL 10 SECOND;
select '12:00:00.4566'::Time(6) + INTERVAL 100 SECOND;

select '12:00:00.4566'::Time(6) + INTERVAL '1-2' YEAR_MONTH;
select '12:00:00.4566'::Time(6) + INTERVAL '1 10:10:10' DAY_TIME;

select '12:00:00.4566'::Time(2) - INTERVAL 24 HOUR;
select '12:00:00.4566'::Time(6) - INTERVAL 10 HOUR;

select '12:00:00.4566'::Time(6) - INTERVAL 10 MINUTE;
select '12:00:00.4566'::Time(6) - INTERVAL 100 MINUTE;

select '12:00:00.4566'::Time(6) - INTERVAL 10 SECOND;
select '12:00:00.4566'::Time(6) - INTERVAL 100 SECOND;

select '12:00:00.4566'::Time(6) - INTERVAL '1-2' YEAR_MONTH;
select '12:00:00.4566'::Time(6) - INTERVAL '1 10:10:10' DAY_TIME;

select '12:00:00.4566'::Time(6) - INTERVAL 10 SECOND + INTERVAL 10 MINUTE;
