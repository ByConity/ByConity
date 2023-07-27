SELECT CONVERT_TZ('2022-01-01 12:00:00','+00:00','+10:00');
SELECT CONVERT_TZ('2022-01-01 12:00:00','+00:00','-10:00');
SELECT CONVERT_TZ('2022-01-01 12:00:00','+00:00','+00:30');
SELECT CONVERT_TZ('2022-01-01 12:00:00','+00:00','-00:30');

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.test_convert_tz;
CREATE TABLE test.test_convert_tz (
  id Int64,
  str String,
  from_tz String,
  to_tz String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO test.test_convert_tz VALUES (0, '2022-01-01 12:00:00','+00:00','+10:00'), (1, '2022-01-01 12:00:00','+00:00','-10:00'), (2, '2022-01-01 12:00:00','+00:00','+00:30'), (3, '2022-01-01 12:00:00','+00:00','-00:30');
SELECT id, CONVERT_TZ(str, from_tz, to_tz) FROM test.test_convert_tz;