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

SELECT ADDTIME(DATE '2022-01-01','01:01:01');
SELECT ADDTIME(TIME '01:02:03', '04:05:06');
SELECT ADDTIME(DATETIME '2022-01-01 01:02:03', '04:05:06');
SELECT ADDTIME(TIMESTAMP '2022-01-01 01:02:03', '04:05:06');
SELECT ADDTIME('2022-12-31 01:02:03', '04:05:06');
SELECT ADDTIME(TIME '23:59:59', '00:00:01');

SELECT SUBTIME(DATE '2022-01-01','01:01:01');
SELECT SUBTIME(TIME '01:02:03', '04:05:06');
SELECT SUBTIME(DATETIME '2022-01-01 01:02:03', '04:05:06');
SELECT SUBTIME(TIMESTAMP '2022-01-01 01:02:03', '04:05:06');
SELECT SUBTIME('2022-12-31 01:02:03', '04:05:06');
SELECT SUBTIME(TIME '23:59:59', '00:00:01');

SELECT ADDTIME(DATE '2022-12-31', '24:00:00'); -- { serverError 6 }
SELECT ADDTIME(DATE '2022-01-01', '25:00:00'); -- { serverError 6 }
SELECT SUBTIME(DATE '2022-12-31', '24:00:00');  -- { serverError 6 }
SELECT SUBTIME(DATE '2022-01-01', '25:00:00');  -- { serverError 6 }

use test;
DROP TABLE IF EXISTS test.time_table;
CREATE TABLE test.time_table
(
    val1 UInt64, 
    val2 TIME,
    val3 DATE,
    val4 DATETIME
) ENGINE=CnchMergeTree() ORDER BY val1;

INSERT INTO test.time_table VALUES(0, '01:02:03', '2022-01-01', '2022-01-01 00:00:00');
INSERT INTO test.time_table VALUES(1, '23:02:03', '2022-01-01', '2022-12-31 23:00:00');
INSERT INTO test.time_table VALUES(2, '23:02:03', '2022-01-01', '2022-01-01 00:00:00');
SELECT val1, ADDTIME(val2, val2), ADDTIME(val3, val2), ADDTIME(val4, val2) FROM test.time_table ORDER BY val1 ASC;
SELECT val1, SUBTIME(val2, val2), SUBTIME(val3, val2), SUBTIME(val4, val2) FROM test.time_table ORDER BY val1 ASC;
DROP TABLE test.time_table;
