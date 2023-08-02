DROP TABLE IF EXISTS test.lambda_udf_2;
CREATE TABLE test.lambda_udf_2
(
    a UInt64,
    b Float64,
    c String,
    d String,
    e Tuple(Float64, String),
    f Array(Float64)
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

CREATE DATABASE IF NOT EXISTS db_lambda_udf_2;

INSERT INTO test.lambda_udf_2 (a, b, c, d, e, f)
VALUES (0, 0, 'abc', 'def', (1.0,'sdc'), [4.5, 2.5]) (1, 4.5, 'adcf', 'dew', (2.5,'scdt'), [4.5, 3.5]) (1, 4.5, 'abcc', 'defs', (3.5,'scdtf'), [2.15, 2.5]) (2, 5, 'abwc', 'defv', (3.5,'scdtf'), [2.9, 2.5]) (5, 9, 'abdc', 'deqf', (2.5,'scdt'), [2.3, 2.5]) (5, 99, 'abc', 'def', (2.5,'scdt'), [2.2, 2.5]) (4, 1, 'abc', 'def', (2.5,'scddvt'), [4.5, 11.5]) (2, 1, 'abc', 'def', (2.2,'ssfcdt'), [4.5, 10.5]) (3, 2.1, 'abc', 'def', (2.9,'vfbscdt'), [4.5, 9.5]) (0, -5, 'abc', 'def', (5.5,'cdvct'), [4.5, 7.5]);

DROP FUNCTION IF EXISTS test.test_lambda_tuple;
DROP FUNCTION IF EXISTS db_lambda_udf_2.test_lambda_array;

CREATE FUNCTION test.test_lambda_tuple AS (x) -> IF(empty(x.2), toString(x.1*10), concat(toString(x.1*10), '_', x.2));
CREATE FUNCTION db_lambda_udf_2.test_lambda_array AS (x) -> arrayCumSum(x);

SELECT
  a,
  b,
  c,
  c,
  e,
  f,
  test.test_lambda_tuple(e),
  db_lambda_udf_2.test_lambda_array(f)
FROM
  test.lambda_udf_2
ORDER BY a;

DROP TABLE test.lambda_udf_2;
DROP FUNCTION test.test_lambda_tuple;
DROP FUNCTION db_lambda_udf_2.test_lambda_array;
DROP DATABASE db_lambda_udf_2;