select period_diff(202202,202103);
select period_diff('202202','202103');
select period_diff(202202,'202103');
select period_diff('202202',202103);
select period_diff(999912,000000);
select period_diff(000000,999912);
select period_diff(5, 1);
select period_diff(9805, 9812);
select period_diff(9805, 9704);
select period_diff(000000,-000001); -- { serverError 377 }

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test_period_diff;
CREATE TABLE test_period_diff (
  id Int64,
  date1 String,
  date2 String,
  date3 Int64
)
ENGINE = CnchMergeTree()
ORDER BY id;


INSERT INTO test_period_diff VALUES (0, '202202', '202103', 202103), (1, '999912', '000000', 000000);
SELECT id, period_diff(date1, date2) FROM test_period_diff ORDER BY id;
SELECT id, period_diff(date1, date3) FROM test_period_diff ORDER BY id;
DROP TABLE IF EXISTS test_period_diff;