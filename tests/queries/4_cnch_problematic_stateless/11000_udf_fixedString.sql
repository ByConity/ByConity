CREATE DATABASE IF NOT EXISTS test;

DROP FUNCTION IF EXISTS test.py_script_33;

CREATE FUNCTION test.py_script_33
RETURNS FixedString(12)
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_33(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

$code$;


select test.py_script_33(number) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_33;

DROP TABLE IF EXISTS test.py_fixedstring;

CREATE TABLE test.py_fixedstring
(
    a FixedString(2),
    b FixedString(3)
)
ENGINE = CnchMergeTree()
ORDER BY b;


INSERT INTO test.py_fixedstring(a, b)
VALUES ('aa', 'aaa') ('bb', 'bbb') ('cc', 'ccc') ('dd', 'ddd') ('ee', 'eee') ('ff', 'fff') ('gg', 'ggg') ('ab', 'abc') ('cb', 'cba') ('tt', 'zzz');

CREATE FUNCTION test.py_script_33
RETURNS FixedString(5)
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_33(IUDF):
    @overload
    def process(a, b):
        return a + b

$code$;

select test.py_script_33(a, b) from test.py_fixedstring;

DROP FUNCTION IF EXISTS test.py_script_33;
DROP TABLE IF EXISTS test.py_fixedstring;