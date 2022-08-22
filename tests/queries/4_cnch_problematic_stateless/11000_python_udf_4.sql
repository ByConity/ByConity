CREATE DATABASE IF NOT EXISTS test;
CREATE DATABASE IF NOT EXISTS test_2;

DROP TABLE IF EXISTS python_udf;
CREATE TABLE python_udf
(
    a UInt64,
    b Float64,
    c Nullable(String),
    d String,
    timestamp DateTime('Europe/Moscow'),
    nullable_col Nullable(UInt64)
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO python_udf (a, b, c, d, timestamp, nullable_col)
VALUES (0, 0, NULL, 'def', 1546300800, 1) (1, 4.5, 'adcf', 'dew', '2019-01-11 00:00:00', 2) (1, 4.5, 'abcc', 'defs', '2020-01-01 00:00:00', NULL) (2, 5, 'abwc', 'defv', '2019-01-06 00:00:00', 6) (5, 9, 'abdc', 'deqf', '2019-05-01 00:00:00', NULL) (5, 99, 'abc', 'def', 1546309800, 7) (4, 1, 'abc', 'def', 1546300800, NULL) (2, 1, 'abc', 'def', '2019-02-01 00:00:00', NULL) (3, 2.1, 'abc', 'def', 1546300810, NULL) (0, -5, 'abc', 'def', '2019-01-01 00:00:00', 8);



DROP FUNCTION IF EXISTS test_2.test_python_string_concat;
DROP FUNCTION IF EXISTS test_2.test_lambda_string_concat;

DROP FUNCTION IF EXISTS test_python_sum_3;

CREATE FUNCTION test_python_sum_3 RETURNS Float64 LANGUAGE PYTHON AS
$pikachu$
from iudf import IUDF
from overload import overload

class test_python_sum_3(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        return a + b + 1
$pikachu$;

CREATE FUNCTION test_2.test_python_string_concat RETURNS String LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_string_concat(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        if not a:
            return b
        return a + b
$code$;

CREATE FUNCTION test_2.test_lambda_string_concat AS (x, y) -> IF(empty(x), y, concat(x,y));

DROP FUNCTION IF EXISTS test_lambda_sum;

CREATE FUNCTION test_lambda_sum AS (x, y) -> x+y;

select a+2+5+1, test_lambda_sum(test_python_sum_3(a, 2.0), 5) from python_udf order by a+2+5+1, test_lambda_sum(test_python_sum_3(a, 2.0), 5);

SELECT
  a,
  b,
  c,
  nullable_col,
  isNull(test_python_sum_3(a, nullable_col)),
  test_python_sum_3(a, b),
  test_lambda_sum(test_python_sum_3(a, 2.0), 5),
  test_python_sum_3(2, b),
  toDateTime(test_python_sum_3(2, timestamp), 'Europe/London'),
  abs(test_python_sum_3(a, abs(b))),
  test_python_sum_3(abs(test_python_sum_3(a, abs(b)))),
  test_2.test_python_string_concat(test_2.test_python_string_concat(c, d), '_cool_nested'),
  test_2.test_python_string_concat('', d),
  test_2.test_python_string_concat(c, '_all_good'),
  test_2.test_lambda_string_concat(test_2.test_python_string_concat(c, d), '_py_in_lambda'),
  test_2.test_python_string_concat(test_2.test_lambda_string_concat(c, d), '_lambda_in_py')
FROM
  python_udf
  where test_python_sum_3(a, b) != 0
ORDER BY test_python_sum_3(a, b), test_2.test_python_string_concat(c, '_all_good');

DROP FUNCTION IF EXISTS test_python_sum_3;
DROP FUNCTION IF EXISTS test_lambda_sum;
DROP FUNCTION IF EXISTS test_2.test_python_string_concat;
DROP FUNCTION IF EXISTS test_2.test_lambda_string_concat;
DROP DATABASE test_2;
