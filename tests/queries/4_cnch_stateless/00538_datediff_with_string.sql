SELECT dateDiff('year', '2017-12-31', '2016-01-01');
SELECT dateDiff('year', '2017-12-31', '2017-01-01');
SELECT dateDiff('year', '2017-12-31', '2018-01-01');
SELECT dateDiff('quarter', '2017-12-31', '2016-01-01');
SELECT dateDiff('quarter', '2017-12-31', '2017-01-01');
SELECT dateDiff('quarter', '2017-12-31', '2018-01-01');
SELECT dateDiff('month', '2017-12-31', '2016-01-01');
SELECT dateDiff('month', '2017-12-31', '2017-01-01');
SELECT dateDiff('month', '2017-12-31', '2018-01-01');
SELECT dateDiff('week', '2017-12-31', '2016-01-01');
SELECT dateDiff('week', '2017-12-31', '2017-01-01');
SELECT dateDiff('week', '2017-12-31', '2018-01-01');
SELECT dateDiff('day', '2017-12-31', '2016-01-01');
SELECT dateDiff('day', '2017-12-31', '2017-01-01');
SELECT dateDiff('day', '2017-12-31', '2018-01-01');
SELECT dateDiff('hour', '2017-12-31', '2016-01-01');
SELECT dateDiff('hour', '2017-12-31', '2017-01-01');
SELECT dateDiff('hour', '2017-12-31', '2018-01-01');
SELECT dateDiff('minute', '2017-12-31', '2016-01-01');
SELECT dateDiff('minute', '2017-12-31', '2017-01-01');
SELECT dateDiff('minute', '2017-12-31', '2018-01-01');
SELECT dateDiff('second', '2017-12-31', '2016-01-01');
SELECT dateDiff('second', '2017-12-31', '2017-01-01');
SELECT dateDiff('second', '2017-12-31', '2018-01-01');

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test_datediff;
CREATE TABLE test_datediff (
  id Int64,
  date1 String,
  date2 String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test_datediff VALUES (0, '2017-12-31', '2016-01-01'), (1, '2017-12-31', '2017-01-01'), (2, '2017-12-31', '2018-01-01');
SELECT id, dateDiff('year', date1, date2) FROM test_datediff ORDER BY id;

SET enable_optimizer=1;
SET dialect_type='MYSQL';

SELECT datediff('day', '2017-12-31', '2018-01-01'); -- { serverError 42 }
SELECT timestampdiff('second', '2017-12-31', '2018-01-01');
select datediff('2022-01-22 23:59:59','2022-01-21');
select datediff(datetime '2022-01-22 23:59:59','2022-01-21');
select datediff(datetime '2022-01-22 23:59:59',datetime '2022-01-21');
select datediff(datetime '2022-01-22 23:59:59',datetime '2022-01-21 23:59:59');
select datediff('2022-01-22 23:59:59',timestamp '2022-01-21');
select datediff(timestamp '2022-01-22 23:59:59',timestamp '2022-01-21');
select datediff('2022-01-22 23:59:59',date '2022-01-21');

select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:35:55');
select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:35:56');
select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:35:57');
select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:36:55');
select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:36:56');
select timestampdiff('minute', '2022-01-01 12:34:56', '2022-01-01 12:36:57');

select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 13:34:55');
select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 13:34:56');
select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 13:34:57');
select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 14:34:55');
select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 14:34:56');
select timestampdiff('hour', '2022-01-01 12:34:56', '2022-01-01 14:34:57');

select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-02 12:34:55');
select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-02 12:34:56');
select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-02 12:34:57');
select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-03 12:34:55');
select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-03 12:34:56');
select timestampdiff('day', '2022-01-01 12:34:56', '2022-01-03 12:34:57');

select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-08 12:34:55');
select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-08 12:34:56');
select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-08 12:34:57');
select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-15 12:34:55');
select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-15 12:34:56');
select timestampdiff('week', '2022-01-01 12:34:56', '2022-01-15 12:34:57');

select timestampdiff('month', '2022-01-01 12:34:56', '2022-02-01 12:34:55');
select timestampdiff('month', '2022-01-01 12:34:56', '2022-02-01 12:34:56');
select timestampdiff('month', '2022-01-01 12:34:56', '2022-02-01 12:34:57');
select timestampdiff('month', '2022-01-01 12:34:56', '2022-03-01 12:34:55');
select timestampdiff('month', '2022-01-01 12:34:56', '2022-03-01 12:34:56');
select timestampdiff('month', '2022-01-01 12:34:56', '2022-03-01 12:34:57');

select timestampdiff('year', '2022-01-01 12:34:56', '2023-01-01 12:34:55');
select timestampdiff('year', '2022-01-01 12:34:56', '2023-01-01 12:34:56');
select timestampdiff('year', '2022-01-01 12:34:56', '2023-01-01 12:34:57');
select timestampdiff('year', '2022-01-01 12:34:56', '2024-01-01 12:34:55');
select timestampdiff('year', '2022-01-01 12:34:56', '2024-01-01 12:34:56');
select timestampdiff('year', '2022-01-01 12:34:56', '2024-01-01 12:34:57');

select timestampdiff('minute', '2022-01-01 12:35:55', '2022-01-01 12:34:56');
select timestampdiff('minute', '2022-01-01 12:35:56', '2022-01-01 12:34:56');
select timestampdiff('minute', '2022-01-01 12:35:57', '2022-01-01 12:34:56');
select timestampdiff('minute', '2022-01-01 12:36:55', '2022-01-01 12:34:56');
select timestampdiff('minute', '2022-01-01 12:36:56', '2022-01-01 12:34:56');
select timestampdiff('minute', '2022-01-01 12:36:57', '2022-01-01 12:34:56');

select timestampdiff('hour', '2022-01-01 13:34:55', '2022-01-01 12:34:56');
select timestampdiff('hour', '2022-01-01 13:34:56', '2022-01-01 12:34:56');
select timestampdiff('hour', '2022-01-01 13:34:57', '2022-01-01 12:34:56');
select timestampdiff('hour', '2022-01-01 14:34:55', '2022-01-01 12:34:56');
select timestampdiff('hour', '2022-01-01 14:34:56', '2022-01-01 12:34:56');
select timestampdiff('hour', '2022-01-01 14:34:57', '2022-01-01 12:34:56');

select timestampdiff('day', '2022-01-02 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('day', '2022-01-02 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('day', '2022-01-02 12:34:57', '2022-01-01 12:34:56');
select timestampdiff('day', '2022-01-03 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('day', '2022-01-03 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('day', '2022-01-03 12:34:57', '2022-01-01 12:34:56');

select timestampdiff('week', '2022-01-08 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('week', '2022-01-08 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('week', '2022-01-08 12:34:57', '2022-01-01 12:34:56');
select timestampdiff('week', '2022-01-15 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('week', '2022-01-15 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('week', '2022-01-15 12:34:57', '2022-01-01 12:34:56');

select timestampdiff('month', '2022-02-01 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('month', '2022-02-01 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('month', '2022-02-01 12:34:57', '2022-01-01 12:34:56');
select timestampdiff('month', '2022-03-01 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('month', '2022-03-01 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('month', '2022-03-01 12:34:57', '2022-01-01 12:34:56');

select timestampdiff('year', '2023-01-01 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('year', '2023-01-01 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('year', '2023-01-01 12:34:57', '2022-01-01 12:34:56');
select timestampdiff('year', '2024-01-01 12:34:55', '2022-01-01 12:34:56');
select timestampdiff('year', '2024-01-01 12:34:56', '2022-01-01 12:34:56');
select timestampdiff('year', '2024-01-01 12:34:57', '2022-01-01 12:34:56');

SELECT id, dateDiff(date1, date2) FROM test_datediff ORDER BY id;
SELECT id, timestampdiff('day', date1, date2) FROM test_datediff ORDER BY id;
