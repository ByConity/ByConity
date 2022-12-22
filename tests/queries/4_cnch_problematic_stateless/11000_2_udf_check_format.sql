CREATE DATABASE IF NOT EXISTS "my\0\t`a";
CREATE DATABASE IF NOT EXISTS `my\0\ta\n\b\x.#$%^&_2"`;

DROP TABLE IF EXISTS "my\0\t`a".python_udf_5;
CREATE TABLE "my\0\t`a".python_udf_5
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

INSERT INTO "my\0\t`a".python_udf_5 (a, b, c, d, timestamp, nullable_col)
VALUES (0, 0, NULL, 'def', 1546300800, 1) (1, 4.5, 'adcf', 'dew', '2019-01-11 00:00:00', 2) (1, 4.5, 'abcc', 'defs', '2020-01-01 00:00:00', NULL) (2, 5, 'abwc', 'defv', '2019-01-06 00:00:00', 6) (5, 9, 'abdc', 'deqf', '2019-05-01 00:00:00', NULL) (5, 99, 'abc', 'def', 1546309800, 7) (4, 1, 'abc', 'def', 1546300800, NULL) (2, 1, 'abc', 'def', '2019-02-01 00:00:00', NULL) (3, 2.1, 'abc', 'def', 1546300810, NULL) (0, -5, 'abc', 'def', '2019-01-01 00:00:00', 8);

use "my\0\t`a";

DROP FUNCTION IF EXISTS `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat;
DROP FUNCTION IF EXISTS `my\0\ta\n\b\x.#$%^&_2"`.lambda_string_concat;

DROP FUNCTION IF EXISTS "my\0\t`a".python_sum_3;

CREATE FUNCTION "my\0\t`a".python_sum_3 RETURNS Float64 LANGUAGE PYTHON AS
$pikachu$
from iudf import IUDF
from overload import overload

class python_sum_3(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        return a + b + 1
$pikachu$;

CREATE FUNCTION `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat RETURNS String LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class python_string_concat(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        if not a:
            return b
        return a + b
$code$;

CREATE FUNCTION `my\0\ta\n\b\x.#$%^&_2"`.lambda_string_concat AS (x, y) -> IF(empty(x), y, concat(x,y));

DROP FUNCTION IF EXISTS "my\0\t`a".lambda_sum;

CREATE FUNCTION "my\0\t`a".lambda_sum AS (x, y) -> x+y;

select a+2+5+1, "my\0\t`a".lambda_sum("my\0\t`a".python_sum_3(a, 2.0), 5) from "my\0\t`a".python_udf_5 order by a+2+5+1, "my\0\t`a".lambda_sum("my\0\t`a".python_sum_3(a, 2.0), 5);

SELECT
  a,
  b,
  c,
  nullable_col,
  isNull("my\0\t`a".python_sum_3(a, nullable_col)),
  "my\0\t`a".python_sum_3(a, b),
  "my\0\t`a".lambda_sum("my\0\t`a".python_sum_3(a, 2.0), 5),
  "my\0\t`a".python_sum_3(2, b),
  toDateTime("my\0\t`a".python_sum_3(2, timestamp), 'Europe/London'),
  abs("my\0\t`a".python_sum_3(a, abs(b))),
  "my\0\t`a".python_sum_3(abs("my\0\t`a".python_sum_3(a, abs(b)))),
  `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(`my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(c, d), '_cool_nested'),
  `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat('', d),
  `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(c, '_all_good'),
  `my\0\ta\n\b\x.#$%^&_2"`.lambda_string_concat(`my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(c, d), '_py_in_lambda'),
  `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(`my\0\ta\n\b\x.#$%^&_2"`.lambda_string_concat(c, d), '_lambda_in_py')
FROM
  "my\0\t`a".python_udf_5
  where "my\0\t`a".python_sum_3(a, b) != 0
ORDER BY "my\0\t`a".python_sum_3(a, b), `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat(c, '_all_good');

DROP FUNCTION IF EXISTS "my\0\t`a".python_sum_3;
DROP FUNCTION IF EXISTS "my\0\t`a".lambda_sum;
DROP FUNCTION IF EXISTS `my\0\ta\n\b\x.#$%^&_2"`.python_string_concat;
DROP FUNCTION IF EXISTS `my\0\ta\n\b\x.#$%^&_2"`.lambda_string_concat;
DROP DATABASE `my\0\ta\n\b\x.#$%^&_2"`;
