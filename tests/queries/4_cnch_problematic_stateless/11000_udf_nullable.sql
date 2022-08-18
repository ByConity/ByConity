CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS python_udf_nullable;

CREATE TABLE python_udf_nullable
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
INSERT INTO python_udf_nullable (a, b, c, d)
VALUES (12,2,3,4) (-12,0,13,14) (21,-3276,2,0) (31,3276,33,NULL);

-- 


DROP FUNCTION IF EXISTS test_python_int_nullable;

CREATE FUNCTION test_python_int_nullable RETURNS Nullable(Int64) LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_int_nullable(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d
$code$;

select * from python_udf_nullable;

SELECT
  test_python_int_nullable(a, b, c, d)
FROM
  python_udf_nullable;


DROP FUNCTION IF EXISTS test_python_int_nullable;