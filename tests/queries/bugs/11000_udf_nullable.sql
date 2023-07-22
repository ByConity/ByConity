DROP TABLE IF EXISTS test.python_udf_nullable_20;

CREATE TABLE test.python_udf_nullable_20
(
    a Int8,
    b Int16,
    c Int32,
    d Nullable(Int64)
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

-- 18446744073709551615 in 5th row will get converted to 0 as it overflows
INSERT INTO test.python_udf_nullable_20 (a, b, c, d)
VALUES (12,2,3,4) (-12,0,13,14) (21,-3276,2,0) (31,3276,33,NULL);

-- use test;


DROP FUNCTION IF EXISTS test.test_python_int_nullable_20;

CREATE FUNCTION test.test_python_int_nullable_20 RETURNS Nullable(Int64) LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_int_nullable_20(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d
$code$;

select * from test.python_udf_nullable_20;

SELECT
  test.test_python_int_nullable_20(a, b, c, d)
FROM
  test.python_udf_nullable_20;


DROP FUNCTION IF EXISTS test.test_python_int_nullable_20;
DROP TABLE IF EXISTS test.python_udf_nullable_20;
