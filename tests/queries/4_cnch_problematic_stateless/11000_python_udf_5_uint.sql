CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS python_udf;
CREATE TABLE python_udf
(
    a UInt8,
    b UInt16,
    c UInt32,
    d UInt64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO python_udf (a, b, c, d)
VALUES (255,2,3,4) (10,65535,13,14) (21,22,4294967295,24) (31,32,33,34) (0,0,0,18446744073709551615);



DROP FUNCTION IF EXISTS test_python_uint;

CREATE FUNCTION test_python_uint RETURNS UInt64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_uint(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d
$code$;

SELECT
  sum(a),
  test_python_uint(a, b, c, d)
FROM
  python_udf
GROUP BY test_python_uint(a, b, c, d)
having test_python_uint(a, b, c, d) > 100
ORDER BY test_python_uint(a, b, c, d) desc;

DROP FUNCTION IF EXISTS test_python_uint;
DROP TABLE IF EXISTS python_udf;