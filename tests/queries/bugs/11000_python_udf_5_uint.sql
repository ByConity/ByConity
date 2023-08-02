
DROP TABLE IF EXISTS test.python_udf_5;
CREATE TABLE test.python_udf_5
(
    a UInt8,
    b UInt16,
    c UInt32,
    d UInt64
)
ENGINE = CnchMergeTree()
PRIMARY KEY a
ORDER BY a;

INSERT INTO test.python_udf_5 (a, b, c, d)
VALUES (255,2,3,4) (10,65535,13,14) (21,22,4294967295,24) (31,32,33,34) (0,0,0,18446744073709551615);

use test;

DROP FUNCTION IF EXISTS test.test_python_uint_5;

CREATE FUNCTION test.test_python_uint_5 RETURNS UInt64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload
import os
import time



class test_python_uint_5(IUDF):
    @overload
    def process(a, b, c, d):
        return a + b + c + d
$code$;

SELECT
  sum(a),
  test.test_python_uint_5(a, b, c, d)
FROM
  test.python_udf_5
GROUP BY test.test_python_uint_5(a, b, c, d)
having test.test_python_uint_5(a, b, c, d) > 100
ORDER BY test.test_python_uint_5(a, b, c, d) desc;

DROP FUNCTION IF EXISTS test.test_python_uint_5;
DROP TABLE IF EXISTS test.python_udf_5;

