CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.python_udf;

CREATE TABLE test.python_udf(id Int, val Int) 
ENGINE = CnchMergeTree()
PRIMARY KEY id
ORDER BY id;

INSERT INTO test.python_udf VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);

use test;
DROP FUNCTION IF EXISTS test_python_limit_by;

CREATE FUNCTION test_python_limit_by RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by(IUDF):
    @overload
    def process(a, b):
        return a+b
$code$;

-- cnch doesn't seem to support offset in limit by
SELECT test_python_limit_by(id, 0), id, val FROM test.python_udf ORDER BY id, val LIMIT 2 BY test_python_limit_by(id, 0);

DROP FUNCTION IF EXISTS test_python_limit_by;
DROP TABLE IF EXISTS test.python_udf;