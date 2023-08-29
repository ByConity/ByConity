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
select date_sub(date '2022-01-22 00:00:00',3);
select date_sub(datetime '2022-01-22 00:00:00',3);
select date_sub(timestamp '2022-01-22 00:00:00',3);
select date_sub('2022-01-22 00:00:00',3);
select date_sub(date '2022-01-22 00:00:00','3');
select date_sub(datetime '2022-01-22 00:00:00','3');
select date_sub(timestamp '2022-01-22 00:00:00','3');
select date_sub('2022-01-22 00:00:00','3');

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.dates;
CREATE TABLE test.dates (
  id Int64,
  str String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test.dates VALUES (0, '2022-01-22'), (1, '2022-01-22 12:12:32'), (3, '2022-01-29');

SELECT id, adddate(str, interval '3' day) FROM test.dates ORDER BY id;
SELECT id, date_sub(str, interval '3' day) FROM test.dates ORDER BY id;

DROP TABLE IF EXISTS test.dates;
