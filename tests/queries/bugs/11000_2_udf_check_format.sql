CREATE DATABASE IF NOT EXISTS "my0ta";
CREATE DATABASE IF NOT EXISTS "my0tanbx";

DROP TABLE IF EXISTS "my0ta".python_udf_5;
CREATE TABLE "my0ta".python_udf_5
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

INSERT INTO "my0ta".python_udf_5 (a, b, c, d, timestamp, nullable_col)
VALUES (0, 0, NULL, 'def', 1546300800, 1) (1, 4.5, 'adcf', 'dew', '2019-01-11 00:00:00', 2) (1, 4.5, 'abcc', 'defs', '2020-01-01 00:00:00', NULL) (2, 5, 'abwc', 'defv', '2019-01-06 00:00:00', 6) (5, 9, 'abdc', 'deqf', '2019-05-01 00:00:00', NULL) (5, 99, 'abc', 'def', 1546309800, 7) (4, 1, 'abc', 'def', 1546300800, NULL) (2, 1, 'abc', 'def', '2019-02-01 00:00:00', NULL) (3, 2.1, 'abc', 'def', 1546300810, NULL) (0, -5, 'abc', 'def', '2019-01-01 00:00:00', 8);

use "my0ta";

DROP FUNCTION IF EXISTS "my0tanbx".python_string_concat;
DROP FUNCTION IF EXISTS "my0tanbx".lambda_string_concat;

DROP FUNCTION IF EXISTS "my0ta".python_sum_3;

CREATE FUNCTION "my0ta".python_sum_3 RETURNS Float64 LANGUAGE PYTHON AS
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

CREATE FUNCTION "my0tanbx".python_string_concat RETURNS String LANGUAGE PYTHON AS
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

CREATE FUNCTION "my0tanbx".lambda_string_concat AS (x, y) -> IF(empty(x), y, concat(x,y));

DROP FUNCTION IF EXISTS "my0ta".lambda_sum;

CREATE FUNCTION "my0ta".lambda_sum AS (x, y) -> x+y;

select a+2+5+1, "my0ta".lambda_sum("my0ta".python_sum_3(a, 2.0), 5) from "my0ta".python_udf_5 order by a+2+5+1, "my0ta".lambda_sum("my0ta".python_sum_3(a, 2.0), 5);

SELECT
  a,
  b,
  c,
  nullable_col,
  isNull("my0ta".python_sum_3(a, nullable_col)),
  "my0ta".python_sum_3(a, b),
  "my0ta".lambda_sum("my0ta".python_sum_3(a, 2.0), 5),
  "my0ta".python_sum_3(2, b),
  toDateTime("my0ta".python_sum_3(2, timestamp), 'Europe/London'),
  abs("my0ta".python_sum_3(a, abs(b))),
  "my0ta".python_sum_3(abs("my0ta".python_sum_3(a, abs(b)))),
  "my0tanbx".python_string_concat("my0tanbx".python_string_concat(c, d), '_cool_nested'),
  "my0tanbx".python_string_concat('', d),
  "my0tanbx".python_string_concat(c, '_all_good'),
  "my0tanbx".lambda_string_concat("my0tanbx".python_string_concat(c, d), '_py_in_lambda'),
  "my0tanbx".python_string_concat("my0tanbx".lambda_string_concat(c, d), '_lambda_in_py')
FROM
  "my0ta".python_udf_5
  where "my0ta".python_sum_3(a, b) != 0
ORDER BY "my0ta".python_sum_3(a, b), "my0tanbx".python_string_concat(c, '_all_good');

DROP FUNCTION IF EXISTS "my0ta".python_sum_3;
DROP FUNCTION IF EXISTS "my0ta".lambda_sum;
DROP FUNCTION IF EXISTS "my0tanbx".python_string_concat;
DROP FUNCTION IF EXISTS "my0tanbx".lambda_string_concat;
DROP TABLE "my0ta".python_udf_5;
DROP DATABASE "my0ta";
DROP DATABASE "my0tanbx";
