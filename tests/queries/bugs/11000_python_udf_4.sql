CREATE DATABASE IF NOT EXISTS db_python_udf_4;

DROP TABLE IF EXISTS test.python_udf_4;
CREATE TABLE test.python_udf_4
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

INSERT INTO test.python_udf_4 (a, b, c, d, timestamp, nullable_col)
VALUES (0, 0, NULL, 'def', 1546300800, 1) (1, 4.5, 'adcf', 'dew', '2019-01-11 00:00:00', 2) (1, 4.5, 'abcc', 'defs', '2020-01-01 00:00:00', NULL) (2, 5, 'abwc', 'defv', '2019-01-06 00:00:00', 6) (5, 9, 'abdc', 'deqf', '2019-05-01 00:00:00', NULL) (5, 99, 'abc', 'def', 1546309800, 7) (4, 1, 'abc', 'def', 1546300800, NULL) (2, 1, 'abc', 'def', '2019-02-01 00:00:00', NULL) (3, 2.1, 'abc', 'def', 1546300810, NULL) (0, -5, 'abc', 'def', '2019-01-01 00:00:00', 8);

use test;

DROP FUNCTION IF EXISTS db_python_udf_4.test_string_concat_python_udf_4;
DROP FUNCTION IF EXISTS db_python_udf_4.test_lambda_string_concat_udf_4;

DROP FUNCTION IF EXISTS test_sum_python_udf_4;

CREATE FUNCTION test_sum_python_udf_4 RETURNS Float64 LANGUAGE PYTHON AS
$pikachu$
from iudf import IUDF
from overload import overload

class test_sum_python_udf_4(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        return a + b + 1
$pikachu$;

CREATE FUNCTION db_python_udf_4.test_string_concat_python_udf_4 RETURNS String LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_string_concat_python_udf_4(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        if not a:
            return b
        return a + b
$code$;

CREATE FUNCTION db_python_udf_4.test_lambda_string_concat_udf_4 AS (x, y) -> IF(empty(x), y, concat(x,y));

DROP FUNCTION IF EXISTS test_lambda_sum_udf_4;

CREATE FUNCTION test_lambda_sum_udf_4 AS (x, y) -> x+y;

select a+2+5+1, test_lambda_sum_udf_4(test_sum_python_udf_4(a, 2.0), 5) from test.python_udf_4 order by a+2+5+1, test_lambda_sum_udf_4(test_sum_python_udf_4(a, 2.0), 5);

SELECT
  a,
  b,
  c,
  nullable_col,
  isNull(test_sum_python_udf_4(a, nullable_col)),
  test_sum_python_udf_4(a, b),
  test_lambda_sum_udf_4(test_sum_python_udf_4(a, 2.0), 5),
  test_sum_python_udf_4(2, b),
  toDateTime(test_sum_python_udf_4(2, timestamp), 'Europe/London'),
  abs(test_sum_python_udf_4(a, abs(b))),
  test_sum_python_udf_4(abs(test_sum_python_udf_4(a, abs(b)))),
  db_python_udf_4.test_string_concat_python_udf_4(db_python_udf_4.test_string_concat_python_udf_4(c, d), '_cool_nested'),
  db_python_udf_4.test_string_concat_python_udf_4('', d),
  db_python_udf_4.test_string_concat_python_udf_4(c, '_all_good'),
  db_python_udf_4.test_lambda_string_concat_udf_4(db_python_udf_4.test_string_concat_python_udf_4(c, d), '_py_in_lambda'),
  db_python_udf_4.test_string_concat_python_udf_4(db_python_udf_4.test_lambda_string_concat_udf_4(c, d), '_lambda_in_py')
FROM
  test.python_udf_4
  where test_sum_python_udf_4(a, b) != 0
ORDER BY test_sum_python_udf_4(a, b), db_python_udf_4.test_string_concat_python_udf_4(c, '_all_good');

DROP FUNCTION IF EXISTS test_sum_python_udf_4;
DROP FUNCTION IF EXISTS test_lambda_sum_udf_4;
DROP FUNCTION IF EXISTS db_python_udf_4.test_string_concat_python_udf_4;
DROP FUNCTION IF EXISTS db_python_udf_4.test_lambda_string_concat_udf_4;
DROP DATABASE db_python_udf_4;
