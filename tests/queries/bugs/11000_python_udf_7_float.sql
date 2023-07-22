
DROP TABLE IF EXISTS test.python_udf_7;
CREATE TABLE test.python_udf_7
(
    a Int32,
    b Int64,
    c Float32,
    d Float64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.python_udf_7 (a, b, c, d)
VALUES (127,2,3.5,4) (-128,65535,13,14.78999) (21,-32768,4294967295.343454,24.121) (31,32767,33,34.233) (0,0,0,18446744073709551614.1) (0,0,2147483647,0.999) (0,0,0.001,-9223372036854775807.09) (0,0,0,9223372036854775806.12);

use test;

DROP FUNCTION IF EXISTS test_python_float_7;

-- If the return type is Float32, it will have different behavior than clickhouse summation when it overflows which is obvious.
CREATE FUNCTION test_python_float_7 RETURNS Float64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_float_7(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d + 1.0
$code$;

SELECT
  a+b+c+d+1.0,
  test_python_float_7(a, b, c, d)
FROM
  test.python_udf_7
ORDER BY test_python_float_7(a, b, c, d) desc;

DROP FUNCTION IF EXISTS test_python_float_7;
DROP TABLE IF EXISTS test.python_udf_7;