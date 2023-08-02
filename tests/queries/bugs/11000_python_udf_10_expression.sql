DROP TABLE IF EXISTS test.python_udf_10;
CREATE TABLE test.python_udf_10
(
    col1 Int8,
    col2 Int16,
    col3 Int32,
    col4 Int64
)
ENGINE = CnchMergeTree()
PRIMARY KEY col1
ORDER BY col1;

INSERT INTO test.python_udf_10 (col1, col2, col3, col4)
VALUES (127,2,3,4) (-128,65535,13,14) (21,-32768,1,24) (31,32767,33,34) (0,0,0,2) (0,0,3,0) (0,0,0,-4) (0,0,0,5);

use test;

DROP FUNCTION IF EXISTS test_python_expression_10;

CREATE FUNCTION test_python_expression_10 RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_expression_10(IUDF):
    @overload
    def process(a, b, c, d):
    #the reason for doing d+c+b+a instead of a+b+c+d is to make sure type conversion doesnt affect result
        return d + c + b + a;
$code$;

SELECT
    col1,
    col2,
    col3,
    col4,
    10*(10*(col1+col2+col3+col4+col2+col3+col4) + col2 + col3 + col4) as col5,
    10*test_python_expression_10(10*test_python_expression_10(col1, col2, col3, col4+col2+col3+col4), col2, col3, col4) as col6,
    col1+col2+col3+col4+col2+col3+col4 as col7,
    test_python_expression_10(col1+col2+col3+col4, col2, col3, col4) as col8
FROM
  test.python_udf_10
ORDER BY -1*test_python_expression_10(col1*-1, col2, col3, col4) desc;

DROP FUNCTION IF EXISTS test_python_expression_10;
DROP TABLE IF EXISTS test.python_udf_10;