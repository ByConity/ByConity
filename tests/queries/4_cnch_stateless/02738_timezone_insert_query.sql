set session_timezone='Europe/Moscow';
SELECT timezone();
-- There is no explicit time zone specified for column d
CREATE TABLE test_timezone (d DateTime) Engine=CnchMergeTree() order by d;
-- When inserted, the current time zone of the session is resolved
INSERT INTO test_timezone VALUES ('2000-01-01 01:00:00');
-- The time zone of the session is also used when d is selected
SELECT d FROM test_timezone;
-- The filter in the where condition also uses the time zone of the session
SELECT d FROM test_timezone where d='2000-01-01 01:00:00';
SELECT '---------------------';

set session_timezone='Asia/Shanghai';
SELECT timezone();
-- After changing the session time zone, d will use the current session time zone display，Moscow +3, Shanghai+8
SELECT * FROM test_timezone;
-- where conditions in the filter, the session timezone will be used, so there will not be a result
SELECT * FROM test_timezone where d = '2000-01-01 01:00:00';
SELECT '---------------------';

-- When inserting new data in the new Shanghai time zone, the Shanghai time zone will be resolved
INSERT INTO test_timezone VALUES ('2000-01-01 01:00:00');
-- The result of the query is 1 o'clock in Shanghai and 6 o'clock in Shanghai which equals 1 o'clock in Moscow
SELECT * FROM test_timezone order by d;

SELECT '---------------------';
-- Here, toDateTime(time string) will be resolved using the session time zone, so 2 points of Shanghai will be inserted
INSERT INTO test_timezone VALUES (toDateTime('2000-01-01 02:00:00'));
-- toDateTime(d) will use the time zone of session too
SELECT toDateTime(d) FROM test_timezone WHERE d = toDateTime('2000-01-01 02:00:00');
DROP TABLE test_timezone;