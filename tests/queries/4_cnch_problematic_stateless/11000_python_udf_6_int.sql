CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS python_udf;
CREATE TABLE python_udf
(
    a Int8,
    b Int16,
    c Int32,
    d Int64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

-- 18446744073709551615 in 5th row will get converted to 0 as it overflows
INSERT INTO python_udf (a, b, c, d)
VALUES (12,2,3,4) (-12,6553,13,14) (21,-3276,429496729,24) (31,3276,33,34) (0,0,0,1844674407370955161) (0,0,214748364,0) (0,0,0,-922337203685477580) (0,0,0,922337203685477580);



DROP FUNCTION IF EXISTS test_python_int;

CREATE FUNCTION test_python_int RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_int(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d
$code$;

SELECT
  sum(a),
  test_python_int(a, b, c, d)
FROM
  python_udf
GROUP BY test_python_int(a, b, c, d)
having test_python_int(a, b, c, d) < 9223372036854775807 and test_python_int(a, b, c, d) > -9223372036854775808
ORDER BY test_python_int(a, b, c, d) desc;

DROP FUNCTION IF EXISTS test_python_int;

CREATE FUNCTION test_python_int RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_int(IUDF):
    @overload
    def process(a, b, c, d):
        print(d)
        return a + b + c + d + 200
$code$;

select a + b + c + d + 200, test_python_int(a, b, c, d) FROM python_udf order by test_python_int(a, b, c, d);

DROP FUNCTION IF EXISTS test_python_int;
DROP TABLE IF EXISTS python_udf;