-- This explicitly specifies that column d uses the UTC time zone
CREATE TABLE test_timezone (d DateTime('UTC')) Engine=CnchMergeTree() order by d;
-- When inserting, the UTC time zone specified by the column is used
INSERT INTO test_timezone VALUES ('2000-01-01 01:00:00');
-- When d is selected, the time zone specified by the column is also used
SELECT d FROM test_timezone;
-- Filters in the where condition also use the time zone specified by the column
SELECT d FROM test_timezone where d='2000-01-01 01:00:00';
SELECT '---------------------';

set session_timezone='Asia/Shanghai';
SELECT timezone();
-- The query result remains unchanged after the session time zone is changed
SELECT * FROM test_timezone;
SELECT * FROM test_timezone where d = '2000-01-01 01:00:00';
SELECT '---------------------';

-- In the new session time zone, new data is inserted, and the new data is still parsed using the UTC time zone of the column
INSERT INTO test_timezone VALUES ('2000-01-01 01:00:00');
-- The query result will get two '2000-01-01 01:00:00'
SELECT * FROM test_timezone;

SELECT '---------------------';
-- Here, toDateTime(time string) will be resolved using the session time zone, so 2 o'clock Shanghai time will be inserted
INSERT INTO test_timezone VALUES (toDateTime('2000-01-01 02:00:00'));
-- toDateTime(d) will use the time zone of column d
SELECT toDateTime(d) FROM test_timezone WHERE d = toDateTime('2000-01-01 02:00:00');

DROP TABLE test_timezone;
