SET dialect_type='MYSQL';
select adddate(date '2022-01-22',interval '3' day);
select adddate(timestamp '2022-01-22',interval '3' day);
select adddate(datetime '2022-01-22',interval '3' day);
select adddate('2022-01-22',interval '3' day);
select adddate(datetime '2022-01-22',interval '3' second);
select adddate(datetime '2022-01-22',interval '3' minute);
select adddate(datetime '2022-01-22',interval '3' hour);
select adddate(datetime '2022-01-22',interval '3' month);
select adddate(datetime '2022-01-22',interval '3' year);
select adddate(datetime '2022-01-22',interval '-3' minute_second);
select adddate(datetime '2022-01-22',interval '3:03' minute_second);
select adddate(datetime '2022-01-22',interval '303' minute_second);
select adddate(datetime '2022-01-22',interval '303:' minute_second);
select adddate(datetime '2022-01-22',interval ':303' minute_second);
select adddate(datetime '2022-01-22',interval '303:303' minute_second);
select adddate(datetime '2022-01-22',interval 'a' minute_second);
select adddate(datetime '2022-01-22',interval '01:01:01' minute_second); -- { serverError 377 }
select adddate(datetime '2022-01-22',interval '01:01:10' hour_second);
select adddate(datetime '2022-01-22',interval '10' hour_second);
select adddate(datetime '2022-01-22',interval '01:10' hour_second);
select adddate(datetime '2022-01-22',interval '01:' hour_second);
select adddate(datetime '2022-01-22',interval '465:283:2121' hour_second);
select adddate(datetime '2022-01-22',interval '1:1:1:1' hour_second); -- { serverError 377 }
select adddate(datetime '2022-01-22',interval '00:10' hour_minute);
select adddate(datetime '2022-01-22',interval ':10' hour_minute);
select adddate(datetime '2022-01-22',interval '10:' hour_minute);
select adddate(datetime '2022-01-22',interval '100' hour_minute);
select adddate(datetime '2022-01-22',interval '100:100' hour_minute);
select adddate(datetime '2022-01-22',interval '10:100:100' hour_minute); -- { serverError 377 }
select adddate(datetime '2022-01-22',interval '1 01:01:10' day_second);
select adddate(datetime '2022-01-22',interval '01:01:10' day_second);
select adddate(datetime '2022-01-22',interval '01:10' day_second);
select adddate(datetime '2022-01-22',interval '10' day_second);
select adddate(datetime '2022-01-22',interval '20 340:322:200' day_second);
select adddate(datetime '2022-01-22',interval '1 20 340:322:200' day_second); -- { serverError 377 }
select adddate(datetime '2022-01-22',interval '1 01:01' day_minute);
select adddate(datetime '2022-01-22',interval '01:01' day_minute);
select adddate(datetime '2022-01-22',interval '01' day_minute);
select adddate(datetime '2022-01-22',interval '34 43674:321' day_minute);
select adddate(datetime '2022-01-22',interval '34 43674:321:120' day_minute); -- { serverError 377 }
select adddate(datetime '2022-01-22',interval '1 01' day_hour);
select adddate(datetime '2022-01-22',interval '1 ' day_hour);
select adddate(datetime '2022-01-22',interval ' ' day_hour);
select adddate(datetime '2022-01-22',interval '1 1 1' day_hour); -- { serverError 377 }
select adddate(datetime '2022-01-22 12:32:01',interval '2 2' year_month);
select adddate(datetime '2022-01-22 12:32:01',interval '2 ' year_month);
select adddate(datetime '2022-01-22 12:32:01',interval ' 2' year_month);
select date_add('2022-01-22 00:00:00',interval '-:::::2 2' year_month);
select date_add('2022-01-22 00:00:00',interval ':::::2 2' year_month);
select date_add('2022-01-22 00:00:00',interval '::-:::::2 2' year_month);
select date_add('2022-01-22 00:00:00',interval '::::::::-2 2' year_month);
select date_add('2022-01-22 00:00:00',interval '        -2 2' year_month);
select date_add('2022-01-22 00:00:00',interval '        - 2 2' year_month);
select date_add('2022-01-22 00:00:00',interval '    ::    -:: 2 2' year_month);
select adddate(datetime '2022-01-22 12:32:01',interval '2 2:2' year_month); -- { serverError 377 }
select adddate('2022-01-22',3);
select adddate(datetime '2022-01-22 12:12:32',3);
select adddate(timestamp '2022-01-22 12:12:32',3);
select adddate('2022-01-22 12:12:32',3);
select adddate('2022-01-22','3');
select adddate(datetime '2022-01-22 12:12:32','3');
select adddate(timestamp '2022-01-22 12:12:32','3');
select adddate('2022-01-22 12:12:32','3');

select date_sub(date '2022-01-22',interval '3' day);
select date_sub(timestamp '2022-01-22 00:00:00',interval '3' day);
select date_sub(datetime '2022-01-22 00:00:00',interval '3' day);
select date_sub('2022-01-22 00:00:00',interval '3' day);
select date_sub('2022-01-22 00:00:00',interval '3' second);
select date_sub('2022-01-22 00:00:00',interval '3' minute);
select date_sub('2022-01-22 00:00:00',interval '3' hour);
select date_sub('2022-01-22 00:00:00',interval '3' month);
select date_sub('2022-01-22 00:00:00',interval '3' year);
select date_sub('2022-01-22 00:00:00',interval '01:10' minute_second);
select date_sub('2022-01-22 00:00:00',interval '01:01:10' hour_second);
select date_sub('2022-01-22 00:00:00',interval '01:01' hour_minute);
select date_sub('2022-01-22 00:00:00',interval '1 01:01:10' day_second);
select date_sub('2022-01-22 00:00:00',interval '1 01:01' day_minute);
select date_sub('2022-01-22 00:00:00',interval '1 01' day_hour);
select date_sub('2022-01-22 00:00:00',interval '2 2' year_month);
select date_sub('2022-01-22 00:00:00',interval '::::-2 2' year_month);
select date_sub('2022-01-22 00:00:00',interval '2 -2' year_month);
select date_sub(date '2022-01-22 00:00:00',3);
select date_sub(datetime '2022-01-22 00:00:00',3);
select date_sub(timestamp '2022-01-22 00:00:00',3);
select date_sub('2022-01-22 00:00:00',3);
select date_sub(date '2022-01-22 00:00:00','3');
select date_sub(datetime '2022-01-22 00:00:00','3');
select date_sub(timestamp '2022-01-22 00:00:00','3');
select date_sub('2022-01-22 00:00:00','3');

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS dates;
CREATE TABLE dates (
  id Int64,
  str String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO dates VALUES (0, '2022-01-22'), (1, '2022-01-22 12:12:32'), (3, '2022-01-29');

SELECT id, adddate(str, interval '3' day) FROM dates ORDER BY id;
SELECT id, date_sub(str, interval '3' day) FROM dates ORDER BY id;

DROP TABLE IF EXISTS dates;

set enable_implicit_arg_type_convert=1;
select adddate(20220122,interval '3' day);
select timestampadd(second,1,20220102121212);
set enable_implicit_arg_type_convert=0;
