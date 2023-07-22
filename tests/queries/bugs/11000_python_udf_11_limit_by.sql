DROP TABLE IF EXISTS test.python_udf_11;

CREATE TABLE test.python_udf_11(id Int, val Int) 
ENGINE = CnchMergeTree()
PRIMARY KEY id
ORDER BY id;

INSERT INTO test.python_udf_11 VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);

use test;
DROP FUNCTION IF EXISTS test_python_limit_by_11;

CREATE FUNCTION test_python_limit_by_11 RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by_11(IUDF):
    @overload
    def process(a, b):
        return a+b
$code$;

SELECT test_python_limit_by_11(id, 0), id, val FROM test.python_udf_11 ORDER BY id, val LIMIT 2 BY test_python_limit_by_11(id, 0);

DROP FUNCTION IF EXISTS test_python_limit_by_11;
DROP TABLE IF EXISTS test.python_udf_11;