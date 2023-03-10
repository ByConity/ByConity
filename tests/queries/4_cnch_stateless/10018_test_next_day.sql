select next_day('2019-09-09', 'Mo');
select next_day('2019-09-09', 'Tu');
select next_day('2019-09-09', 'we');
select next_day('2019-09-09', 'th');
select next_day('2019-09-09 00:00:00', 'Fri');
select next_day('2019-09-09 00:00:00', 'Sat');
select next_day('2019-09-09 00:00:00', 'Sunday');
select next_day(toDate('2019-09-11', 'Asia/Shanghai'), 'Mo');
select next_day(toDate('2019-09-11', 'Asia/Shanghai'), 'Tu');
select next_day(toDate('2019-09-11', 'Asia/Shanghai'), 'we');
select next_day(toDate('2019-09-11', 'Asia/Shanghai'), 'th');
select next_day(toDateTime('2019-09-11 00:00:00', 'Asia/Shanghai'), 'Fri');
select next_day(toDateTime('2019-09-11 00:00:00', 'Asia/Shanghai'), 'Sat');
select next_day(materialize(toDateTime('2019-09-11 00:00:00', 'Asia/Shanghai')), materialize('Sunday'));
