select toHour('11:20:30'::Time);
select Hour(Time'11:20:30');
select Hour('11:20:30'::Time);
select toMinute('11:20:30'::Time);
select Minute('11:20:30'::Time);
select Minute(Time'11:20:30');
select toSecond('11:20:30'::Time);
select Second('11:20:30'::Time);
select Second(Time'11:20:30');

select addYears('11:20:30'::Time(3), 5);
select addYears('11:20:30'::Time(4), 20);
select subtractYears('11:20:30'::Time(5), 5);
select subtractYears('11:20:30'::Time(6), 20);

select addMonths('11:20:30'::Time(3), 5);
select addMonths('11:20:30'::Time(4), 20);
select subtractMonths('11:20:30'::Time(5), 5);
select subtractMonths('11:20:30'::Time(6), 20);

select addDays('11:20:30'::Time(3), 5);
select addDays('11:20:30'::Time(4), 20);
select subtractDays('11:20:30'::Time(5), 5);
select subtractDays('11:20:30'::Time(6), 20);

select addHours('11:20:30'::Time(3), 5);
select addHours('11:20:30'::Time(4), 20);
select subtractHours('11:20:30'::Time(5), 5);
select subtractHours('11:20:30'::Time(6), 20);

select addMinutes('11:20:30'::Time(2), 50);
select addMinutes('11:20:30'::Time(3), 2000);
select subtractMinutes('11:20:30'::Time, 50);
select subtractMinutes('11:20:30'::Time(1), 5000);

select addSeconds('11:20:30'::Time, 500);
select addSeconds('11:20:30'::Time(2), 200000);
select subtractSeconds('11:20:30'::Time, 500);
select subtractSeconds('11:20:30'::Time(1), 500000);
