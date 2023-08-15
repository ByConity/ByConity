SELECT day('2021-11-03');
SELECT toMonth('2021-11-03');
SELECT toQuarter('2021-11-03');
SELECT toYear('2021-11-03');
SELECT toRelativeYearNum('2021-11-03');

SELECT day('20211103');
SELECT toMonth('20211103');
SELECT toQuarter('20211103');
SELECT toYear('20211103');
SELECT toRelativeYearNum('20211103');

SELECT day(Time'20:00:00') == day(today());
SELECT toMonth(Time'20:00:00') == toMonth(today());
SELECT toQuarter(Time'20:00:00') == toQuarter(today());
SELECT toYear(Time'20:00:00') == toYear(today());
SELECT extract(year from time '15:23:22') == toYear(today());

SELECT hour(Date'2022-01-01');
SELECT minute(Date'2022-01-01');
SELECT second(Date'2022-01-01');