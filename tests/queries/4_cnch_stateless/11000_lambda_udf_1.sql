DROP TABLE IF EXISTS test.lambda_udf_1;
CREATE TABLE test.lambda_udf_1
(
    a UInt64,
    b Float64,
    c String,
    d String
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

CREATE DATABASE IF NOT EXISTS db_lambda_udf_1;

INSERT INTO test.lambda_udf_1 (a, b, c, d)
VALUES (0, 0, 'abc', 'def') (1, 4.5, 'adcf', 'dew') (1, 4.5, 'abcc', 'defs') (2, 5, 'abwc', 'defv') (5, 9, 'abdc', 'deqf') (5, 99, 'abc', 'def') (4, 1, 'abc', 'def') (2, 1, 'abc', 'def') (3, 2.1, 'abc', 'def') (0, -5, 'abc', 'def');

use test;
DROP FUNCTION IF EXISTS test_lambda_sum;
DROP FUNCTION IF EXISTS db_lambda_udf_1.test_lambda_string_concat;
CREATE FUNCTION test_lambda_sum AS (x, y) -> x+y;
CREATE FUNCTION db_lambda_udf_1.test_lambda_string_concat AS (x, y) -> IF(empty(x), y, concat(x,y));

SELECT
  a,
  b,
  test_lambda_sum(a, b),
  test_lambda_sum(a, 2.0),
  test_lambda_sum(2, b),
  abs(test_lambda_sum(a, -5)),
  db_lambda_udf_1.test_lambda_string_concat(c, d),
  db_lambda_udf_1.test_lambda_string_concat('', d),
  db_lambda_udf_1.test_lambda_string_concat(c, '_all_good')
FROM
  test.lambda_udf_1
where test_lambda_sum(a, b) > 5
ORDER BY test_lambda_sum(a, b), db_lambda_udf_1.test_lambda_string_concat(c, '_all_good');

DROP TABLE test.lambda_udf_1;
DROP FUNCTION IF EXISTS test_lambda_sum;
DROP FUNCTION IF EXISTS db_lambda_udf_1.test_lambda_string_concat;
DROP DATABASE db_lambda_udf_1;
