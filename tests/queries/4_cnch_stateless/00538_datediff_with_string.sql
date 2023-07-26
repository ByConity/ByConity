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
DROP TABLE IF EXISTS test.test_datediff;
CREATE TABLE test.test_datediff (
  id Int64,
  date1 String,
  date2 String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test.test_datediff VALUES (0, '2017-12-31', '2016-01-01'), (1, '2017-12-31', '2017-01-01'), (2, '2017-12-31', '2018-01-01');
SELECT id, dateDiff('year', date1, date2) FROM test.test_datediff ORDER BY id;

SET enable_optimizer=1;
SET dialect_type='MYSQL';

select datediff('2022-01-22 23:59:59','2022-01-21');
select datediff(datetime '2022-01-22 23:59:59','2022-01-21');
select datediff(datetime '2022-01-22 23:59:59',datetime '2022-01-21');
select datediff(datetime '2022-01-22 23:59:59',datetime '2022-01-21 23:59:59');
select datediff('2022-01-22 23:59:59',timestamp '2022-01-21');
select datediff(timestamp '2022-01-22 23:59:59',timestamp '2022-01-21');
select datediff('2022-01-22 23:59:59',date '2022-01-21');

SELECT id, dateDiff(date1, date2) FROM test.test_datediff ORDER BY id;